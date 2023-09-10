package googlecloudstorage

import (
	"context"
	_ "embed"
	"fmt"
	"os"
	"sync"

	"cloud.google.com/go/storage"
	"github.com/gofrs/uuid"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/instill-ai/connector/pkg/base"
	"github.com/instill-ai/connector/pkg/configLoader"

	connectorPB "github.com/instill-ai/protogen-go/vdp/connector/v1alpha"
)

const (
	vendorName  = "googlecloudstorage"
	taskUpload  = "TASK_UPLOAD"
	credsEnvVar = "GOOGLE_APPLICATION_CREDENTIALS"
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

func NewClient(keyFilePath string) (*storage.Client, error) {
	os.Setenv(credsEnvVar, keyFilePath)
	client, err := storage.NewClient(context.Background())
	return client, err
}

func (c *Connection) getBucketName() string {
	return c.Config.GetFields()["bucket_name"].GetStringValue()
}

func (c *Connection) getKeyPath() string {
	return c.Config.GetFields()["key_path"].GetStringValue()
}

func (c *Connection) Execute(inputs []*structpb.Struct) ([]*structpb.Struct, error) {
	outputs := []*structpb.Struct{}
	task := inputs[0].GetFields()["task"].GetStringValue()

	if err := c.ValidateInput(inputs, task); err != nil {
		return nil, err
	}
	client, err := NewClient(c.getKeyPath())
	if err != nil {
		return nil, err
	}
	for _, input := range inputs {
		var output *structpb.Struct
		switch task {
		case taskUpload:
			objectName := input.GetFields()["object_name"].GetStringValue()
			data := input.GetFields()["data"].GetStringValue()
			err = uploadToGCS(client, c.getBucketName(), objectName, data)
			if err != nil {
				return nil, err
			}
			output = &structpb.Struct{Fields: map[string]*structpb.Value{"status": {Kind: &structpb.Value_StringValue{StringValue: "success"}}}}
		}
		outputs = append(outputs, output)
	}
	if err := c.ValidateOutput(outputs, task); err != nil {
		return nil, err
	}
	return outputs, nil
}

func (c *Connection) Test() (connectorPB.ConnectorResource_State, error) {
	client, err := NewClient(c.getKeyPath())
	if err != nil {
		return connectorPB.ConnectorResource_STATE_ERROR, fmt.Errorf("error creating GCS client: %v", err)
	}
	if client == nil {
		return connectorPB.ConnectorResource_STATE_DISCONNECTED, fmt.Errorf("GCS client is nil")
	}
	defer client.Close()
	return connectorPB.ConnectorResource_STATE_CONNECTED, nil
}
