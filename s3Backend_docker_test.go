// +build docker

package s3backend_test

import (
	"fmt"
	"log"
	"net/http"
	"testing"
	"time"

	cache "flamingo.me/flamingo/v3/core/cache"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/ory/dockertest"
)

var (
	s3_dockerTestPool     *dockertest.Pool
	s3_dockerTestResource *dockertest.Resource
)

var (
	// Assert the interface is matched
	_ cache.Backend = &cache.S3Backend{}
)

// setup an redis docker-container for integration tests
func setup_DefaultBackendTestCase_S3Backend() {
	var err error
	s3_dockerTestPool, err = dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	options := &dockertest.RunOptions{
		Repository: "minio/minio",
		Tag:        "RELEASE.2019-10-12T01-39-57Z",
		Cmd:        []string{"server", "/data"},
		Env:        []string{"MINIO_ACCESS_KEY=MYACCESSKEY", "MINIO_SECRET_KEY=MYSECRETKEY"},
	}

	s3_dockerTestResource, err = s3_dockerTestPool.RunWithOptions(options)
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
		panic(err)
	}

	// minio takes a moment for starting.
	tries := 0
	for true {
		_, err := http.Get(fmt.Sprintf("http://127.0.0.1:%v", s3_dockerTestResource.GetPort("9000/tcp")))
		if err == nil {
			return
		}
		tries += 1
		if tries >= 10 {
			panic(err)
		}
		time.Sleep(time.Second * 1)
	}
}

// teardown the redis docker-container
func teardown_DefaultBackendTestCase_S3Backend() {
	err := s3_dockerTestPool.Purge(s3_dockerTestResource)
	if err != nil {
		log.Fatalf("Error purging docker resources: %s", err)
	}
}

func Test_RunDefaultBackendTestCase_S3Backend(t *testing.T) {

	setup_DefaultBackendTestCase_S3Backend()

	s3Config := &aws.Config{
		Credentials:      credentials.NewStaticCredentials("MYACCESSKEY", "MYSECRETKEY", ""),
		Endpoint:         aws.String(fmt.Sprintf("http://127.0.0.1:%v", s3_dockerTestResource.GetPort("9000/tcp"))),
		Region:           aws.String("us-east-1"),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	}

	backend := s3backend.NewS3Backend(s3Config, "prefix", "test-bucket", "s3BackendTest")
	testcase := cache.NewBackendTestCase(t, backend, false)
	testcase.RunTests()

	teardown_DefaultBackendTestCase_S3Backend() // comment out, if you want to keep the docker-instance running for debugging
}
