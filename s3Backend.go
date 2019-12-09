package s3backend

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"time"

	cache "flamingo.me/flamingo/v3/core/cache"
	"flamingo.me/flamingo/v3/framework/flamingo"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type (
	// S3Backend instance representation
	S3Backend struct {
		cacheMetrics cache.CacheMetrics
		s3           *s3.S3
		keyPrefix    string
		bucketName   string
		logger       flamingo.Logger
	}

	// S3CacheEntry representation
	S3CacheEntry struct {
		Meta s3CacheEntryMeta
		Data interface{}
	}

	// s3CacheEntryMeta representation
	s3CacheEntryMeta struct {
		Lifetime, Gracetime time.Duration
	}
)

func init() {
	gob.Register(new(S3CacheEntry))
}

// NewS3Backend creates an S3Backend instance
func NewS3Backend(s3Config *aws.Config, keyPrefix string, bucketName string, frontendName string) *S3Backend {
	awsSession, _ := session.NewSession(s3Config)
	s3Service := s3.New(awsSession)

	_, _ = s3Service.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})

	err := s3Service.WaitUntilBucketExists(&s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		panic(err)
	}

	return &S3Backend{
		cacheMetrics: cache.NewCacheMetrics("s3", frontendName),
		keyPrefix:    keyPrefix,
		bucketName:   bucketName,
		logger:       flamingo.NullLogger{},
		s3:           s3Service,
	}
}

// Get entry by tag
func (b *S3Backend) Get(key string) (entry *cache.Entry, found bool) {
	getObjectOut, err := b.s3.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(b.bucketName),
		Key:    aws.String(fmt.Sprintf("/%v/%v", b.keyPrefix, key)),
	})

	if err != nil {
		b.cacheMetrics.countError(fmt.Sprintf("%v", err))
		return nil, false
	}

	body, err := ioutil.ReadAll(getObjectOut.Body)
	if err != nil {
		b.cacheMetrics.countError(fmt.Sprintf("%v", err))
		return nil, false
	}

	s3Entry, err := b.decodeEntry(body)
	if err != nil {
		b.cacheMetrics.countError("DecodeFailed")
		b.logger.WithField("category", "s3Backend").Error(fmt.Sprintf("Error decoding content of key '%v': %v", key, err))
		return nil, false
	}

	b.cacheMetrics.countHit()
	return b.buildResult(s3Entry), true
}

// Set an cache key
func (b *S3Backend) Set(key string, entry *cache.Entry) error {
	s3Entry := b.buildEntry(entry)

	buffer, err := b.encodeEntry(s3Entry)
	if err != nil {
		b.cacheMetrics.countError("EncodeFailed")
		b.logger.WithField("category", "s3Backend").Error("Error encoding: %v: %v", key, s3Entry)
		return err
	}

	_, err = b.s3.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(b.bucketName),
		Key:    aws.String(fmt.Sprintf("/%v/%v", b.keyPrefix, key)),
		Body:   bytes.NewReader(buffer.Bytes()),
	})
	if err != nil {
		b.cacheMetrics.countError("SetFailed")
		b.logger.WithField("category", "s3Backend").Error("Error setting key %v with timeout %v and buffer %v", key, int(entry.Meta.Gracetime.Seconds()), buffer)
		return err
	}

	return nil
}

// Purge an cache key
func (b *S3Backend) Purge(key string) error {
	_, err := b.s3.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(b.bucketName),
		Key:    aws.String(fmt.Sprintf("/%v/%v", b.keyPrefix, key)),
	})
	if err != nil {
		b.logger.WithField("category", "s3Backend").Error(fmt.Sprintf("Failed Purge %v", err))
		return err
	}

	return nil
}

// Flush the whole cache
func (b *S3Backend) Flush() error {
	listObjectsOut, err := b.s3.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(b.bucketName),
		Prefix: aws.String(fmt.Sprintf("/%v", b.keyPrefix)),
	})
	if err != nil {
		b.logger.WithField("category", "s3Backend").Error(fmt.Sprintf("Failed list for purge %v", err))
		return err
	}

	for _, s3Object := range listObjectsOut.Contents {
		_, err := b.s3.DeleteObject(&s3.DeleteObjectInput{
			Bucket: aws.String(b.bucketName),
			Key:    s3Object.Key,
		})
		if err != nil {
			b.logger.WithField("category", "s3Backend").Error(fmt.Sprintf("Failed DEL for key '%v': %v", s3Object.Key, err))
			return err
		}
	}

	return nil
}

func (b *S3Backend) encodeEntry(entry *S3CacheEntry) (*bytes.Buffer, error) {
	buffer := new(bytes.Buffer)
	err := gob.NewEncoder(buffer).Encode(entry)
	if err != nil {
		return nil, err
	}
	return buffer, nil
}

func (b *S3Backend) decodeEntry(content []byte) (*S3CacheEntry, error) {
	buffer := bytes.NewBuffer(content)
	decoder := gob.NewDecoder(buffer)
	entry := new(S3CacheEntry)
	err := decoder.Decode(&entry)
	if err != nil {
		return nil, err
	}

	return entry, err
}

// buildEntry removes unneeded Meta.Tags before encoding
func (b *S3Backend) buildEntry(entry *cache.Entry) *S3CacheEntry {
	return &S3CacheEntry{
		Meta: s3CacheEntryMeta{
			Lifetime:  entry.Meta.Lifetime,
			Gracetime: entry.Meta.Gracetime,
		},
		Data: entry.Data,
	}
}

// buildResult removes unneeded Meta.Tags before encoding
func (b *S3Backend) buildResult(entry *S3CacheEntry) *cache.Entry {
	return &cache.Entry{
		Meta: cache.Meta{
			Lifetime:  entry.Meta.Lifetime,
			Gracetime: entry.Meta.Gracetime,
		},
		Data: entry.Data,
	}
}
