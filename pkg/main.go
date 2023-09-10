package destination

import (
	"fmt"
	"sync"

	"github.com/gofrs/uuid"
	"github.com/instill-ai/connector-data/pkg/bigquery"
	"github.com/instill-ai/connector-data/pkg/googlecloudstorage"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/instill-ai/connector-data/pkg/airbyte"
	"github.com/instill-ai/connector-data/pkg/pinecone"
	"github.com/instill-ai/connector/pkg/base"
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
			def, err := bigqueryConnector.GetConnectorDefinitionByUid(uid)
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

func (c *Connector) CreateConnection(defUid uuid.UUID, config *structpb.Struct, logger *zap.Logger) (base.IConnection, error) {
	switch {
	case c.airbyteConnector.HasUid(defUid):
		return c.airbyteConnector.CreateConnection(defUid, config, logger)
	case c.pineconeConnector.HasUid(defUid):
		return c.pineconeConnector.CreateConnection(defUid, config, logger)
	case c.bigqueryConnector.HasUid(defUid):
		return c.bigqueryConnector.CreateConnection(defUid, config, logger)
	case c.gcsConnector.HasUid(defUid):
		return c.gcsConnector.CreateConnection(defUid, config, logger)
	default:
		return nil, fmt.Errorf("no destinationConnector uid: %s", defUid)
	}
}
