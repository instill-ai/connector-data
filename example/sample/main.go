package main

import (
	"fmt"

	connectorData "github.com/instill-ai/connector-data/pkg"
	connectorDataAirbyte "github.com/instill-ai/connector-data/pkg/airbyte"
	"go.uber.org/zap"
)

func main() {

	logger, _ := zap.NewDevelopment()
	// It is singleton, should be loaded when connector-backend started
	connector := connectorData.Init(logger, connectorData.ConnectorOptions{
		Airbyte: connectorDataAirbyte.ConnectorOptions{
			MountSourceVDP:     "vdp",
			MountTargetVDP:     "/tmp/vdp",
			MountSourceAirbyte: "airbyte",
			MountTargetAirbyte: "/tmp/airbyte",
			VDPProtocolPath:    "vdp_protocol.yaml",
		},
	})

	// For apis: Get connector definitsion apis
	for _, v := range connector.ListConnectorDefinitions() {
		fmt.Println(v)
	}

	// in connector-backend:
	// if user trigger connectorA
	// ->connectorA.defUid
	// ->connectorA.configuration
	// connection, _ := connector.CreateExecution(uuid.FromStringOrNil("70d8664a-d512-4517-a5e8-5d4da81756a7"), &structpb.Struct{})
	// _, err := connection.Execute(nil)

	// if err != nil {
	// 	fmt.Println(err)
	// }

}
