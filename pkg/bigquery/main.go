package bigquery

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"sync"

	"cloud.google.com/go/bigquery"
	"github.com/gofrs/uuid"
	"go.uber.org/zap"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/instill-ai/component/pkg/base"
	"github.com/instill-ai/component/pkg/configLoader"

	connectorPB "github.com/instill-ai/protogen-go/vdp/connector/v1alpha"
)

const (
	vendorName = "bigquery"
	taskInsert = "TASK_INSERT"
)

//go:embed config/definitions.json
var definitionJson []byte

var once sync.Once
var connector base.IConnector

type Connector struct {
	base.BaseConnector
	options ConnectorOptions
}

type Connection struct {
	base.BaseExecution
	connector *Connector
}

type ConnectorOptions struct {
}

func Init(logger *zap.Logger, options ConnectorOptions) base.IConnector {
	once.Do(func() {
		loader := configLoader.InitJSONSchema(logger)
		connDefs, err := loader.LoadConnector(vendorName, connectorPB.ConnectorType_CONNECTOR_TYPE_DATA, definitionJson)
		if err != nil {
			panic(err)
		}
		connector = &Connector{
			BaseConnector: base.BaseConnector{Logger: logger},
			options:       options,
		}
		for idx := range connDefs {
			err := connector.AddConnectorDefinition(uuid.FromStringOrNil(connDefs[idx].GetUid()), connDefs[idx].GetId(), connDefs[idx])
			if err != nil {
				logger.Warn(err.Error())
			}
		}
	})
	return connector
}

func (c *Connector) CreateExecution(defUid uuid.UUID, config *structpb.Struct, logger *zap.Logger) (base.IExecution, error) {
	def, err := c.GetConnectorDefinitionByUid(defUid)
	if err != nil {
		return nil, err
	}
	return &Connection{
		BaseExecution: base.BaseExecution{
			Logger: logger, DefUid: defUid,
			Config:                config,
			OpenAPISpecifications: def.Spec.OpenapiSpecifications,
		},
		connector: c,
	}, nil
}

func NewClient(jsonKey, projectID string) (*bigquery.Client, error) {
	return bigquery.NewClient(context.Background(), projectID, option.WithCredentialsJSON([]byte(jsonKey)))
}

func (c *Connection) getJSONKey() string {
	return c.Config.GetFields()["json_key"].GetStringValue()
}
func (c *Connection) getProjectID() string {
	return c.Config.GetFields()["project_id"].GetStringValue()
}
func (c *Connection) getDatasetID() string {
	return c.Config.GetFields()["dataset_id"].GetStringValue()
}
func (c *Connection) getTableName() string {
	return c.Config.GetFields()["table_name"].GetStringValue()
}

func (c *Connection) Execute(inputs []*structpb.Struct) ([]*structpb.Struct, error) {
	outputs := []*structpb.Struct{}
	task := inputs[0].GetFields()["task"].GetStringValue()
	if err := c.ValidateInput(inputs, task); err != nil {
		return nil, err
	}
	client, err := NewClient(c.getJSONKey(), c.getProjectID())
	if err != nil || client == nil {
		return nil, fmt.Errorf("error creating BigQuery client: %v", err)
	}
	defer client.Close()

	for _, input := range inputs {
		var output *structpb.Struct
		switch task {
		case taskInsert:
			datasetID := c.getDatasetID()
			tableName := c.getTableName()
			tableRef := client.Dataset(datasetID).Table(tableName)
			metaData, err := tableRef.Metadata(context.Background())
			if err != nil {
				return nil, err
			}
			valueSaver, err := getDataSaver(input, metaData.Schema)
			if err != nil {
				return nil, err
			}
			err = insertDataToBigQuery(c.getProjectID(), datasetID, tableName, valueSaver, client)
			if err != nil {
				return nil, err
			}
			output = &structpb.Struct{Fields: map[string]*structpb.Value{"status": {Kind: &structpb.Value_StringValue{StringValue: "success"}}}}
		default:
			return nil, errors.New("unsupported task type")
		}
		outputs = append(outputs, output)
	}
	if err = c.ValidateOutput(outputs, task); err != nil {
		return nil, err
	}
	return outputs, nil
}

func (co *Connector) Test(defUid uuid.UUID, config *structpb.Struct, logger *zap.Logger) (connectorPB.ConnectorResource_State, error) {
	ex, err := co.CreateExecution(defUid, config, logger)
	if err != nil {
		return connectorPB.ConnectorResource_STATE_ERROR, err
	}
	c, _ := ex.(*Connection)
	client, err := NewClient(c.getJSONKey(), c.getProjectID())
	if err != nil || client == nil {
		return connectorPB.ConnectorResource_STATE_ERROR, fmt.Errorf("error creating BigQuery client: %v", err)
	}
	defer client.Close()
	if client.Project() == c.getProjectID() {
		return connectorPB.ConnectorResource_STATE_CONNECTED, nil
	}
	return connectorPB.ConnectorResource_STATE_DISCONNECTED, errors.New("project ID does not match")
}
