//go:build integration
// +build integration

package destination

import (
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/instill-ai/connector/pkg/base"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"
)

var (
	jsonKey []byte
	gcsCon  base.IConnection
	logger  *zap.Logger
)

func init() {
	jsonKey, _ = ioutil.ReadFile("test_artifacts/gcp_key.json")
	gcsConfig := &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"json_key":    {Kind: &structpb.Value_StringValue{StringValue: string(jsonKey)}},
			"bucket_name": {Kind: &structpb.Value_StringValue{StringValue: "gcs-connector-sjne"}},
		}}
	logger, _ = zap.NewDevelopment()
	c := Init(logger, ConnectorOptions{})
	uuid := c.ListConnectorDefinitionUids()
	gcsCon, _ = c.CreateConnection(uuid[len(uuid)-1], gcsConfig, nil)
}

func TestGCSUpload(*testing.T) {
	objectName := "test_dog_img.jpg"
	fileContents, _ := ioutil.ReadFile("test_artifacts/dog.jpg")
	base64Str := base64.StdEncoding.EncodeToString(fileContents)
	input := []*structpb.Struct{{
		Fields: map[string]*structpb.Value{
			"task":        {Kind: &structpb.Value_StringValue{StringValue: "TASK_UPLOAD"}},
			"object_name": {Kind: &structpb.Value_StringValue{StringValue: objectName}},
			"data":        {Kind: &structpb.Value_StringValue{StringValue: base64Str}},
		}}}
	op, err := gcsCon.Execute(input)
	fmt.Printf("op: %v, err: %v", op, err)
}

func TestBigQueryInsert(*testing.T) {
	bigQueryConfig := &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"json_key":   {Kind: &structpb.Value_StringValue{StringValue: string(jsonKey)}},
			"project_id": {Kind: &structpb.Value_StringValue{StringValue: "prj-c-connector-879a"}},
			"dataset_id": {Kind: &structpb.Value_StringValue{StringValue: "test_data_set"}},
			"table_name": {Kind: &structpb.Value_StringValue{StringValue: "test_table"}},
			"schema": {Kind: &structpb.Value_ListValue{
				ListValue: &structpb.ListValue{
					Values: []*structpb.Value{
						{
							Kind: &structpb.Value_StructValue{
								StructValue: &structpb.Struct{
									Fields: map[string]*structpb.Value{
										"name": {Kind: &structpb.Value_StringValue{StringValue: "id"}},
										"type": {Kind: &structpb.Value_StringValue{StringValue: "INT64"}},
									},
								},
							},
						},
						{
							Kind: &structpb.Value_StructValue{
								StructValue: &structpb.Struct{
									Fields: map[string]*structpb.Value{
										"name": {Kind: &structpb.Value_StringValue{StringValue: "name"}},
										"type": {Kind: &structpb.Value_StringValue{StringValue: "STRING"}},
									},
								},
							},
						},
					},
				},
			}},
		}}

	logger, _ = zap.NewDevelopment()
	c := Init(logger, ConnectorOptions{})
	uuid := c.ListConnectorDefinitionUids()
	bigQueryCon, _ := c.CreateConnection(uuid[len(uuid)-2], bigQueryConfig, nil)
	input := []*structpb.Struct{{
		Fields: map[string]*structpb.Value{
			"task": {Kind: &structpb.Value_StringValue{StringValue: "TASK_INSERT"}},
			"input": {Kind: &structpb.Value_StructValue{
				StructValue: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"id":   {Kind: &structpb.Value_NumberValue{NumberValue: 3}},
						"name": {Kind: &structpb.Value_StringValue{StringValue: "Harsh"}},
					},
				},
			}},
		}}}
	op, err := bigQueryCon.Execute(input)
	fmt.Printf("op: %v, err: %v", op, err)
}
