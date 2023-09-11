package bigquery

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/bigquery"
	"google.golang.org/protobuf/types/known/structpb"
)

func insertDataToBigQuery(projectID, datasetID, tableName string, schema bigquery.Schema, data []map[string]bigquery.Value, client *bigquery.Client) error {
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
	if err := inserter.Put(ctx, data); err != nil {
		return fmt.Errorf("error inserting data: %v", err)
	}
	fmt.Printf("Data inserted into %s.%s.%s.\n", projectID, datasetID, tableName)
	return nil
}

func getDataAndSchema(input *structpb.Struct) ([]map[string]bigquery.Value, bigquery.Schema, error) {
	schemaVal := input.GetFields()["schema"].GetListValue()
	inputObj := input.GetFields()["input"].GetStructValue()
	schemaJSON, _ := json.Marshal(schemaVal)
	schema, err := bigquery.SchemaFromJSON(schemaJSON)
	if err != nil {
		return nil, nil, err
	}
	dataMap := map[string]bigquery.Value{}
	for _, sc := range schema {
		dataMap[sc.Name] = inputObj.GetFields()[sc.Name].AsInterface()
	}
	return []map[string]bigquery.Value{dataMap}, schema, nil
}
