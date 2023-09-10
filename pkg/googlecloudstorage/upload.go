package googlecloudstorage

import (
	"context"
	"io"

	"cloud.google.com/go/storage"
)

func uploadToGCS(client *storage.Client, bucketName, objectName, data string) error {
	wc := client.Bucket(bucketName).Object(objectName).NewWriter(context.Background())
	if _, err := io.WriteString(wc, data); err != nil {
		return err
	}
	return wc.Close()
}
