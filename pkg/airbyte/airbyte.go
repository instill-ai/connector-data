package airbyte

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/ghodss/yaml"
	"github.com/santhosh-tekuri/jsonschema/v5"
	"go.uber.org/zap"
)

// AirbyteMessage defines the AirbyteMessage protocol  as in
// https://github.com/airbytehq/airbyte/blob/master/airbyte-protocol/protocol-models/src/main/resources/airbyte_protocol/airbyte_protocol.yaml#L13-L49
type AirbyteMessage struct {
	Type   string                `json:"type"`
	Record *AirbyteRecordMessage `json:"record"`
}

// AirbyteRecordMessage defines the RECORD type of AirbyteMessage, AirbyteRecordMessage, protocol as in (without namespace field)
// https://github.com/airbytehq/airbyte/blob/master/airbyte-protocol/protocol-models/src/main/resources/airbyte_protocol/airbyte_protocol.yaml#L50-L70
type AirbyteRecordMessage struct {
	Stream    string          `json:"stream"`
	Data      json.RawMessage `json:"data"`
	EmittedAt int64           `json:"emitted_at"`
}

// AirbyteCatalog defines the AirbyteCatalog protocol as in:
// https://github.com/airbytehq/airbyte/blob/master/airbyte-protocol/protocol-models/src/main/resources/airbyte_protocol/airbyte_protocol.yaml#L212-L222
type AirbyteCatalog struct {
	Streams []AirbyteStream `json:"streams"`
}

// AirbyteStream defines the AirbyteStream protocol as in (without namespace field):
// https://github.com/airbytehq/airbyte/blob/master/airbyte-protocol/protocol-models/src/main/resources/airbyte_protocol/airbyte_protocol.yaml#L223-L260
type AirbyteStream struct {
	Name                    string          `json:"name"`
	JSONSchema              json.RawMessage `json:"json_schema"`
	SupportedSyncModes      []string        `json:"supported_sync_modes"`
	SourceDefinedCursor     bool            `json:"source_defined_cursor"`
	DefaultCursorField      []string        `json:"default_cursor_field"`
	SourceDefinedPrimaryKey [][]string      `json:"source_defined_primary_key"`
}

// ConfiguredAirbyteCatalog defines the ConfiguredAirbyteCatalog protocol as in:
// https://github.com/airbytehq/airbyte/blob/master/airbyte-protocol/protocol-models/src/main/resources/airbyte_protocol/airbyte_protocol.yaml#L261-L271
type ConfiguredAirbyteCatalog struct {
	Streams []ConfiguredAirbyteStream `json:"streams"`
}

// ConfiguredAirbyteStream defines the ConfiguredAirbyteStream protocol  as in:
// https://github.com/airbytehq/airbyte/blob/master/airbyte-protocol/protocol-models/src/main/resources/airbyte_protocol/airbyte_protocol.yaml#L272-L299
type ConfiguredAirbyteStream struct {
	Stream              *AirbyteStream `json:"stream"`
	SyncMode            string         `json:"sync_mode"`
	CursorField         []string       `json:"cursor_field"`
	DestinationSyncMode string         `json:"destination_sync_mode"`
	PrimaryKey          []string       `json:"primary_key"`
}

// TaskOutputAirbyteCatalog stores the pre-defined task AirbyteCatalog
var TaskOutputAirbyteCatalog AirbyteCatalog

// TODO: add this in vdp_protocol
const dataSchema = `
{
	"$schema": "http://json-schema.org/draft-04/schema#",
	"type": "object",
	"properties": {
		"data_mapping_index": {
			"type": "string"
		},
		"texts": {
			"type": "array"
		},
		"structured_data": {
			"type": "object"
		},
		"metadata": {
			"type": "object"
		}
	},
	"required": ["data_mapping_index"]
}
`

// InitAirbyteCatalog reads all task AirbyteCatalog files and stores the JSON content in the global TaskAirbyteCatalog variable
func InitAirbyteCatalog(logger *zap.Logger, vdpProtocolPath string) {

	yamlFile, err := os.ReadFile(vdpProtocolPath)
	if err != nil {
		logger.Fatal(fmt.Sprintf("%#v\n", err.Error()))
	}

	jsonSchemaBytes, err := yaml.YAMLToJSON(yamlFile)
	if err != nil {
		logger.Fatal(fmt.Sprintf("%#v\n", err.Error()))
	}

	compiler := jsonschema.NewCompiler()

	err = compiler.AddResource("protocol.json", strings.NewReader(dataSchema))
	if err != nil {
		logger.Fatal(fmt.Sprintf("%#v\n", err.Error()))
	}

	_, err = compiler.Compile("protocol.json")
	if err != nil {
		logger.Fatal(fmt.Sprintf("%#v\n", err.Error()))
	}

	// Initialise TaskOutputAirbyteCatalog.Streams[0]
	TaskOutputAirbyteCatalog.Streams = []AirbyteStream{
		{
			Name:                "vdp",
			JSONSchema:          jsonSchemaBytes,
			SupportedSyncModes:  []string{"full_refresh", "incremental"},
			SourceDefinedCursor: false,
		},
	}

}
