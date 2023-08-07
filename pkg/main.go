package destination

import (
	"fmt"
	"sync"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/instill-ai/connector-data/pkg/airbyte"

	"github.com/instill-ai/connector/pkg/base"
)

var once sync.Once
var connector base.IConnector

type Connector struct {
	base.BaseConnector
	airbyteConnector base.IConnector
}

type ConnectorOptions struct {
	Airbyte airbyte.ConnectorOptions
}

func Init(logger *zap.Logger, options ConnectorOptions) base.IConnector {
	once.Do(func() {

		airbyteConnector := airbyte.Init(logger, options.Airbyte)

		connector = &Connector{
			BaseConnector:    base.BaseConnector{Logger: logger},
			airbyteConnector: airbyteConnector,
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

	})
	return connector
}

func (c *Connector) CreateConnection(defUid uuid.UUID, config *structpb.Struct, logger *zap.Logger) (base.IConnection, error) {
	switch {
	case c.airbyteConnector.HasUid(defUid):
		return c.airbyteConnector.CreateConnection(defUid, config, logger)
	default:
		return nil, fmt.Errorf("no destinationConnector uid: %s", defUid)
	}
}
