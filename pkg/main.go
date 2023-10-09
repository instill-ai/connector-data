package destination

import (
	"sync"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/instill-ai/component/pkg/base"
	"github.com/instill-ai/connector-data/pkg/airbyte"
	"github.com/instill-ai/connector-data/pkg/bigquery"
	"github.com/instill-ai/connector-data/pkg/googlecloudstorage"
	"github.com/instill-ai/connector-data/pkg/pinecone"

	connectorPB "github.com/instill-ai/protogen-go/vdp/connector/v1alpha"
)

var once sync.Once
var connector base.IConnector

type Connector struct {
	base.Connector
	connectorUIDMap map[uuid.UUID]base.IConnector
}

type ConnectorOptions struct {
	Airbyte airbyte.ConnectorOptions
}

func Init(logger *zap.Logger, options ConnectorOptions) base.IConnector {
	once.Do(func() {
		connector = &Connector{
			Connector:       base.Connector{Component: base.Component{Logger: logger}},
			connectorUIDMap: map[uuid.UUID]base.IConnector{},
		}

		connector.(*Connector).ImportDefinitions(airbyte.Init(logger, options.Airbyte))
		connector.(*Connector).ImportDefinitions(pinecone.Init(logger))
		connector.(*Connector).ImportDefinitions(bigquery.Init(logger))
		connector.(*Connector).ImportDefinitions(googlecloudstorage.Init(logger))
	})
	return connector
}
func (c *Connector) ImportDefinitions(con base.IConnector) {
	for _, v := range con.ListConnectorDefinitions() {
		err := c.AddConnectorDefinition(v)
		if err != nil {
			panic(err)
		}
		c.connectorUIDMap[uuid.FromStringOrNil(v.Uid)] = con
	}
}

func (c *Connector) CreateExecution(defUID uuid.UUID, task string, config *structpb.Struct, logger *zap.Logger) (base.IExecution, error) {
	return c.connectorUIDMap[defUID].CreateExecution(defUID, task, config, logger)
}

func (c *Connector) Test(defUid uuid.UUID, config *structpb.Struct, logger *zap.Logger) (connectorPB.ConnectorResource_State, error) {
	return c.connectorUIDMap[defUid].Test(defUid, config, logger)
}
