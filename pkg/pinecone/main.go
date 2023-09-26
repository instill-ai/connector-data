package pinecone

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/instill-ai/component/pkg/base"
	"github.com/instill-ai/component/pkg/configLoader"

	connectorPB "github.com/instill-ai/protogen-go/vdp/connector/v1alpha"
)

const (
	vendorName   = "pinecone"
	reqTimeout   = time.Second * 60 * 5
	taskQuery    = "QUERY"
	taskUpsert   = "UPSERT"
	jsonMimeType = "application/json"
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

type Client struct {
	APIKey     string
	HTTPClient HTTPClient
}

type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
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

// NewClient initializes a new Stability AI client
func NewClient(apiKey string) Client {
	tr := &http.Transport{
		DisableKeepAlives: true,
	}
	return Client{APIKey: apiKey, HTTPClient: &http.Client{Timeout: reqTimeout, Transport: tr}}
}

func (c *Connection) getAPIKey() string {
	return c.Config.GetFields()["api_key"].GetStringValue()
}

func (c *Connection) getURL() string {
	return c.Config.GetFields()["url"].GetStringValue()
}

// sendReq is responsible for making the http request with to given URL, method, and params and unmarshalling the response into given object.
func (c *Client) sendReq(reqURL, method string, params interface{}, respObj interface{}) error {
	var req *http.Request
	data, err := json.Marshal(params)
	if err != nil {
		return err
	}
	req, err = http.NewRequest(method, reqURL, bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", jsonMimeType)
	req.Header.Add("Accept", jsonMimeType)
	req.Header.Add("Api-Key", c.APIKey)
	http.DefaultClient.Timeout = reqTimeout
	res, err := c.HTTPClient.Do(req)
	if res != nil && res.Body != nil {
		defer res.Body.Close()
	}
	if err != nil || res == nil {
		return fmt.Errorf("error occurred: %v, while calling URL: %s, request body: %s", err, reqURL, data)
	}
	bytes, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("non-200 status code: %d, while calling URL: %s, response body: %s", res.StatusCode, reqURL, bytes)
	}
	if err = json.Unmarshal(bytes, &respObj); err != nil {
		err = fmt.Errorf("error in json decode: %s, while calling URL: %s, response body: %s", err, reqURL, bytes)
	}
	return err
}

func (c *Connection) Execute(inputs []*structpb.Struct) ([]*structpb.Struct, error) {
	// TODO: validata input/output

	client := NewClient(c.getAPIKey())
	outputs := []*structpb.Struct{}
	// TODO: Check all inputs should be the same task.
	task := inputs[0].GetFields()["task"].GetStringValue()

	if err := c.ValidateInput(inputs, task); err != nil {
		return nil, err
	}

	for _, input := range inputs {
		var output *structpb.Struct
		switch task {
		case taskQuery:
			inputStruct := QueryReq{}
			err := base.ConvertFromStructpb(input, &inputStruct)
			if err != nil {
				return nil, err
			}
			url := c.getURL() + "/query"
			resp := QueryResp{}
			err = client.sendReq(url, http.MethodPost, inputStruct, &resp)
			if err != nil {
				return nil, err
			}
			output, err = base.ConvertToStructpb(resp)
			if err != nil {
				return nil, err
			}
		case taskUpsert:
			inputStruct := UpsertReq{}
			err := base.ConvertFromStructpb(input, &inputStruct)
			if err != nil {
				return nil, err
			}
			url := c.getURL() + "/vectors/upsert"
			resp := UpsertResp{}
			err = client.sendReq(url, http.MethodPost, inputStruct, &resp)
			if err != nil {
				return nil, err
			}
			output, err = base.ConvertToStructpb(resp)
			if err != nil {
				return nil, err
			}
		}
		outputs = append(outputs, output)
	}
	if err := c.ValidateOutput(outputs, task); err != nil {
		return nil, err
	}
	return outputs, nil
}

func (c *Connector) Test(defUid uuid.UUID, config *structpb.Struct, logger *zap.Logger) (connectorPB.ConnectorResource_State, error) {
	//TODO: change this
	return connectorPB.ConnectorResource_STATE_CONNECTED, nil
}
