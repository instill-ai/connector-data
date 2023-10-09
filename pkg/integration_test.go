//go:build integration
// +build integration

package destination

import (
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"testing"

	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/instill-ai/component/pkg/base"
)

var (
	jsonKey []byte
	gcsCon  base.IExecution
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
	uuid := c.ListDefinitionUids()
	gcsCon, _ = c.CreateExecution(uuid[len(uuid)-1], gcsConfig, nil)
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
		}}

	logger, _ = zap.NewDevelopment()
	c := Init(logger, ConnectorOptions{})
	uuids := c.ListDefinitionUids()
	uuid := uuids[len(uuids)-2]
	bigQueryCon, _ := c.CreateExecution(uuid, bigQueryConfig, nil)
	state, err := c.Test(uuid, bigQueryConfig, nil)
	fmt.Printf("state: %v, err: %v", state, err)
	input := []*structpb.Struct{{
		Fields: map[string]*structpb.Value{
			"task": {Kind: &structpb.Value_StringValue{StringValue: "TASK_INSERT"}},
			"input": {Kind: &structpb.Value_StructValue{
				StructValue: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"id":   {Kind: &structpb.Value_NumberValue{NumberValue: 5}},
						"name": {Kind: &structpb.Value_StringValue{StringValue: "Tobias"}},
					},
				},
			}},
		}}}
	op, err := bigQueryCon.Execute(input)
	fmt.Printf("op: %v, err: %v", op, err)
}
