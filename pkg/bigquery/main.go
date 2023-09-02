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

	"github.com/instill-ai/connector/pkg/base"
	"github.com/instill-ai/connector/pkg/configLoader"

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
	base.BaseConnection
	connector *Connector
}

type ConnectorOptions struct {
}

func Init(logger *zap.Logger, options ConnectorOptions) base.IConnector {
	once.Do(func() {
		loader := configLoader.InitJSONSchema(logger)
		connDefs, err := loader.Load(vendorName, connectorPB.ConnectorType_CONNECTOR_TYPE_DATA, definitionJson)
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

func (c *Connector) CreateConnection(defUid uuid.UUID, config *structpb.Struct, logger *zap.Logger) (base.IConnection, error) {
	def, err := c.GetConnectorDefinitionByUid(defUid)
	if err != nil {
		return nil, err
	}
	return &Connection{
		BaseConnection: base.BaseConnection{
			Logger: logger, DefUid: defUid,
			Config:     config,
			Definition: def,
		},
		connector: c,
	}, nil
}

func (c *Connection) getAPIKey() string {
	return c.Config.GetFields()["api_key"].GetStringValue()
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
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, c.getProjectID(), option.WithAPIKey(c.getAPIKey()))
	if err != nil || client == nil {
		return nil, fmt.Errorf("error creating BigQuery client: %v", err)
	}
	defer client.Close()

	for _, input := range inputs {
		var output *structpb.Struct
		switch task {
		case taskInsert:
			data, schemaJSON, err := getDataAndSchema(input)
			if err != nil {
				return nil, err
			}
			err = insertDataToBigQuery(c.getProjectID(), c.getDatasetID(), c.getTableName(), schemaJSON, data, client)
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

func (c *Connection) Test() (connectorPB.ConnectorResource_State, error) {
	client, err := bigquery.NewClient(context.Background(), c.getProjectID(), option.WithAPIKey(c.getAPIKey()))
	if err != nil || client == nil {
		return connectorPB.ConnectorResource_STATE_ERROR, fmt.Errorf("error creating BigQuery client: %v", err)
	}
	defer client.Close()
	if client.Project() == c.getProjectID() {
		return connectorPB.ConnectorResource_STATE_CONNECTED, nil
	}
	return connectorPB.ConnectorResource_STATE_DISCONNECTED, errors.New("project ID does not match")
}
