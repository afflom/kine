package uor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	homedir "github.com/mitchellh/go-homedir"

	"github.com/k3s-io/kine/pkg/server"
	"github.com/k3s-io/kine/pkg/tls"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sirupsen/logrus"
	"github.com/uor-framework/uor-client-go/content/layout"
	"github.com/uor-framework/uor-client-go/ocimanifest"
	"github.com/uor-framework/uor-client-go/registryclient"
	"github.com/uor-framework/uor-client-go/registryclient/orasclient"
)

type UContext struct {
	Client     registryclient.Client
	Connection string
	Cache      *layout.Layout
	LastWrite  ocispec.Descriptor
}

func New(ctx context.Context, connection string, tlsInfo tls.Config) (server.Backend, error) {

	var cacheDir string
	cacheEnv := os.Getenv("UOR_CACHE")
	if cacheEnv != "" {
		cacheDir = cacheEnv
	} else {
		home, err := homedir.Dir()
		if err != nil {
			return nil, err
		}
		cacheDir = filepath.Join(home, ".uor", "cache")
	}
	cache, err := layout.NewWithContext(ctx, cacheDir)
	if err != nil {
		return nil, err
	}

	client, err := orasclient.NewClient(
		orasclient.SkipTLSVerify(true),
		orasclient.WithPlainHTTP(true),
		orasclient.WithAuthConfigs(nil),
	)
	if err != nil {
		return nil, err
	}

	desc, err := client.AddContent(ctx, ocimanifest.UORSchemaMediaType, []byte(connection), nil)
	if err != nil {
		return nil, err
	}

	configDesc, err := client.AddContent(ctx, ocimanifest.UORConfigMediaType, []byte("{}"), nil)
	if err != nil {
		return nil, err
	}
	_, err = client.AddManifest(ctx, connection, configDesc, nil, desc)
	if err != nil {
		return nil, err
	}

	_, err = client.Save(ctx, connection, cache)
	if err != nil {
		return nil, fmt.Errorf("client save error for reference %s: %v", connection, err)
	}

	published, err := client.Push(ctx, cache, connection)
	if err != nil {
		return nil, fmt.Errorf("error publishing content to %s: %v", connection, err)
	}

	return &UContext{
		Client:     client,
		Connection: connection,
		Cache:      cache,
		LastWrite:  published,
	}, nil

}

func (j *UContext) Start(ctx context.Context) error {
	// See https://github.com/kubernetes/kubernetes/blob/442a69c3bdf6fe8e525b05887e57d89db1e2f3a5/staging/src/k8s.io/apiserver/pkg/storage/storagebackend/factory/etcd3.go#L97
	if _, err := j.Create(ctx, "/registry/health", []byte(`{"health":"true"}`), 0); err != nil {
		if err != server.ErrKeyExists {
			logrus.Errorf("Failed to create health check key: %v", err)
		}
	}
	return nil
}

// Get returns the associated server.KeyValue
func (j *UContext) Get(ctx context.Context, key, rangeEnd string, limit, revision int64) (revRet int64, kvRet *server.KeyValue, errRet error) {
	return 0, nil, nil
}

// Create
func (j *UContext) Create(ctx context.Context, key string, value []byte, lease int64) (revRet int64, errRet error) {

	return 0, nil
}

func (j *UContext) Delete(ctx context.Context, key string, revision int64) (revRet int64, kvRet *server.KeyValue, deletedRet bool, errRet error) {

	return 0, nil, true, nil
}

func (j *UContext) List(ctx context.Context, prefix, startKey string, limit, revision int64) (revRet int64, kvRet []*server.KeyValue, errRet error) {

	return 0, nil, nil
}

// Count returns an exact count of the number of matching keys and the current revision of the database
func (j *UContext) Count(ctx context.Context, prefix string) (revRet int64, count int64, err error) {

	return 0, 0, nil
}

func (j *UContext) Update(ctx context.Context, key string, value []byte, revision, lease int64) (revRet int64, kvRet *server.KeyValue, updateRet bool, errRet error) {
	return 0, nil, false, nil
}

func (j *UContext) Watch(ctx context.Context, prefix string, revision int64) <-chan []*server.Event {

	return nil
}

// DbSize get the kineBucket size from JetStream.
func (j *UContext) DbSize(ctx context.Context) (int64, error) {
	return 0, nil
}
