package bigquery

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
	"google.golang.org/protobuf/types/known/structpb"
)

func insertDataToBigQuery(projectID, datasetID, tableName, schemaJSON string, data []map[string]bigquery.Value, client *bigquery.Client) error {
	ctx := context.Background()
	tableRef := client.Dataset(datasetID).Table(tableName)
	schema, err := bigquery.SchemaFromJSON([]byte(schemaJSON))
	if err != nil {
		return fmt.Errorf("error parsing schema: %v", err)
	}
	// Create the table if it doesn't exist
	_, err = tableRef.Metadata(ctx)
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

func getDataAndSchema(input *structpb.Struct) ([]map[string]bigquery.Value, string, error) {
	//TODO: implement this
	/*
		sample data -
		data := []map[string]bigquery.Value{
			{
				"column1": "Value1",
				"column2": 100,
			},
			{
				"column1": "Value2",
				"column2": 200,
			},
		}
	*/
	return nil, "{}", nil
}
