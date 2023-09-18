package bigquery

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/bigquery"
	"google.golang.org/protobuf/types/known/structpb"
)

type DataSaver struct {
	Schema  bigquery.Schema
	DataMap map[string]bigquery.Value
}

func (v DataSaver) Save() (row map[string]bigquery.Value, insertID string, err error) {
	return v.DataMap, bigquery.NoDedupeID, nil
}

func insertDataToBigQuery(projectID, datasetID, tableName string, schema bigquery.Schema, valueSaver DataSaver, client *bigquery.Client) error {
	ctx := context.Background()
	tableRef := client.Dataset(datasetID).Table(tableName)
	// Create the table if it doesn't exist
	_, err := tableRef.Metadata(ctx)
	if err != nil {
		metaData := &bigquery.TableMetadata{
			Schema: schema,
		}
		if err := tableRef.Create(ctx, metaData); err != nil {
			return fmt.Errorf("error creating table: %v", err)
		}
		fmt.Printf("Table %s.%s.%s created.\n", projectID, datasetID, tableName)
	}
	// Insert data into the table
	inserter := tableRef.Inserter()
	if err := inserter.Put(ctx, valueSaver); err != nil {
		//retry
		err = inserter.Put(ctx, valueSaver)
		if err == nil {
			return nil
		}
		return fmt.Errorf("error inserting data: %v", err)
	}
	fmt.Printf("Data inserted into %s.%s.%s.\n", projectID, datasetID, tableName)
	return nil
}

func getDataAndSchema(input *structpb.Struct) (DataSaver, bigquery.Schema, error) {
	schemaVal := input.GetFields()["schema"].GetListValue()
	inputObj := input.GetFields()["input"].GetStructValue()
	schemaJSON, _ := json.Marshal(schemaVal)
	schema, err := bigquery.SchemaFromJSON(schemaJSON)
	if err != nil {
		return DataSaver{}, nil, err
	}
	dataMap := map[string]bigquery.Value{}
	for _, sc := range schema {
		dataMap[sc.Name] = inputObj.GetFields()[sc.Name].AsInterface()
	}
	return DataSaver{Schema: schema, DataMap: dataMap}, schema, nil
}
