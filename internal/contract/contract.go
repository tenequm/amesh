// Package contract reserves the schema names used by the hub client until generation from api/openapi.yaml.
package contract

type (
	EventStreamItem         struct{}
	HelloRequest            struct{}
	HelloResponse           struct{}
	PeersResponse           struct{}
	SessionRegisterRequest  struct{}
	SessionRegisterResponse struct{}
	HeartbeatRequest        struct{}
	HeartbeatResponse       struct{}
	HumanTurnRequest        struct{}
	HumanTurnResponse       struct{}
	SessionEndRequest       struct{}
	SessionEndResponse      struct{}
	SendRequest             struct{}
	SendResponse            struct{}
	ReportsRequest          struct{}
	ReportsResponse         struct{}
)
