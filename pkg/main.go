package destination

import (
	"fmt"
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
	base.BaseConnector
	airbyteConnector  base.IConnector
	pineconeConnector base.IConnector
	bigqueryConnector base.IConnector
	gcsConnector      base.IConnector
}

type ConnectorOptions struct {
	Airbyte            airbyte.ConnectorOptions
	PineCone           pinecone.ConnectorOptions
	BigQuery           bigquery.ConnectorOptions
	GoogleCloudStorage googlecloudstorage.ConnectorOptions
}

func Init(logger *zap.Logger, options ConnectorOptions) base.IConnector {
	once.Do(func() {
		airbyteConnector := airbyte.Init(logger, options.Airbyte)
		pineconeConnector := pinecone.Init(logger, options.PineCone)
		bigqueryConnector := bigquery.Init(logger, options.BigQuery)
		gcsConnector := googlecloudstorage.Init(logger, options.GoogleCloudStorage)

		connector = &Connector{
			BaseConnector:     base.BaseConnector{Logger: logger},
			airbyteConnector:  airbyteConnector,
			pineconeConnector: pineconeConnector,
			bigqueryConnector: bigqueryConnector,
			gcsConnector:      gcsConnector,
		}

		// TODO: assert no duplicate uid
		// Note: we preserve the order as yaml
		for _, uid := range airbyteConnector.ListConnectorDefinitionUids() {
			def, err := airbyteConnector.GetConnectorDefinitionByUid(uid)
			if err != nil {
				logger.Error(err.Error())
			}
			err = connector.AddConnectorDefinition(uid, def.GetId(), def)
			if err != nil {
				logger.Warn(err.Error())
			}
		}
		for _, uid := range pineconeConnector.ListConnectorDefinitionUids() {
			def, err := pineconeConnector.GetConnectorDefinitionByUid(uid)
			if err != nil {
				logger.Error(err.Error())
			}
			err = connector.AddConnectorDefinition(uid, def.GetId(), def)
			if err != nil {
				logger.Warn(err.Error())
			}
		}
		for _, uid := range bigqueryConnector.ListConnectorDefinitionUids() {
			def, err := bigqueryConnector.GetConnectorDefinitionByUid(uid)
			if err != nil {
				logger.Error(err.Error())
			}
			err = connector.AddConnectorDefinition(uid, def.GetId(), def)
			if err != nil {
				logger.Warn(err.Error())
			}
		}
		for _, uid := range gcsConnector.ListConnectorDefinitionUids() {
			def, err := gcsConnector.GetConnectorDefinitionByUid(uid)
			if err != nil {
				logger.Error(err.Error())
			}
			err = connector.AddConnectorDefinition(uid, def.GetId(), def)
			if err != nil {
				logger.Warn(err.Error())
			}
		}
	})
	return connector
}

func (c *Connector) CreateExecution(defUid uuid.UUID, config *structpb.Struct, logger *zap.Logger) (base.IExecution, error) {
	switch {
	case c.airbyteConnector.HasUid(defUid):
		return c.airbyteConnector.CreateExecution(defUid, config, logger)
	case c.pineconeConnector.HasUid(defUid):
		return c.pineconeConnector.CreateExecution(defUid, config, logger)
	case c.bigqueryConnector.HasUid(defUid):
		return c.bigqueryConnector.CreateExecution(defUid, config, logger)
	case c.gcsConnector.HasUid(defUid):
		return c.gcsConnector.CreateExecution(defUid, config, logger)
	default:
		return nil, fmt.Errorf("no connector uid: %s", defUid)
	}
}

func (c *Connector) Test(defUid uuid.UUID, config *structpb.Struct, logger *zap.Logger) (connectorPB.ConnectorResource_State, error) {
	switch {
	case c.airbyteConnector.HasUid(defUid):
		return c.airbyteConnector.Test(defUid, config, logger)
	case c.pineconeConnector.HasUid(defUid):
		return c.pineconeConnector.Test(defUid, config, logger)
	case c.bigqueryConnector.HasUid(defUid):
		return c.bigqueryConnector.Test(defUid, config, logger)
	case c.gcsConnector.HasUid(defUid):
		return c.gcsConnector.Test(defUid, config, logger)
	default:
		return connectorPB.ConnectorResource_STATE_ERROR, fmt.Errorf("no connector uid: %s", defUid)
	}
}
