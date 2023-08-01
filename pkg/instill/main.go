package instill

import (
	"fmt"
	"sync"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/instill-ai/connector-data/pkg/instill/end"
	"github.com/instill-ai/connector-data/pkg/instill/start"
	"github.com/instill-ai/connector/pkg/base"
)

var once sync.Once
var connector base.IConnector

type Connector struct {
	base.BaseConnector
	instillStartConnector base.IConnector
	instillEndConnector   base.IConnector
}

func Init(logger *zap.Logger) base.IConnector {
	once.Do(func() {

		instillEndConnector := end.Init(logger)
		instillStartConnector := start.Init(logger)

		connector = &Connector{
			BaseConnector:         base.BaseConnector{Logger: logger},
			instillStartConnector: instillStartConnector,
			instillEndConnector:   instillEndConnector,
		}

		// TODO: assert no duplicate uid
		// Note: we preserve the order as yaml
		for _, uid := range instillStartConnector.ListConnectorDefinitionUids() {
			def, err := instillStartConnector.GetConnectorDefinitionByUid(uid)
			if err != nil {
				logger.Error(err.Error())
			}
			connector.AddConnectorDefinition(uid, def.GetId(), def)
		}
		for _, uid := range instillEndConnector.ListConnectorDefinitionUids() {
			def, err := instillEndConnector.GetConnectorDefinitionByUid(uid)
			if err != nil {
				logger.Error(err.Error())
			}
			connector.AddConnectorDefinition(uid, def.GetId(), def)
		}

	})
	return connector
}

func (c *Connector) CreateConnection(defUid uuid.UUID, config *structpb.Struct, logger *zap.Logger) (base.IConnection, error) {
	switch {
	case c.instillStartConnector.HasUid(defUid):
		return c.instillStartConnector.CreateConnection(defUid, config, logger)
	case c.instillEndConnector.HasUid(defUid):
		return c.instillEndConnector.CreateConnection(defUid, config, logger)
	default:
		return nil, fmt.Errorf("no sourceConnector uid: %s", defUid)
	}
}
