// Code generated from api/openapi.yaml by generate.py. DO NOT EDIT.
package contract

import "encoding/json"

type Ms = int64
type UUID = string
type MachineID = string
type SessionKey = string
type EndpointID = string
type Label = string
type MachineName = string
type Address = string
type AddressInput = string
type FromMode = string
type PeerStatus = string
type Kind = string
type Epoch = int64
type Hop struct {
	Count int64 `json:"count"`
	Limit int64 `json:"limit"`
}
type HopChain = []string
type ErrorCode = string
type ErrorBody struct {
	Error       ErrorCode  `json:"error"`
	Message     string     `json:"message"`
	Protocol    *int64     `json:"protocol,omitempty"`
	MinProtocol *int64     `json:"minProtocol,omitempty"`
	MinAmesh    *string    `json:"minAmesh,omitempty"`
	ServerTime  *Ms        `json:"serverTime,omitempty"`
	RetryAfter  *int64     `json:"retryAfter,omitempty"`
	MaxBytes    *int64     `json:"maxBytes,omitempty"`
	Candidates  *[]Address `json:"candidates,omitempty"`
	Hop         *Hop       `json:"hop,omitempty"`
	Epoch       *Epoch     `json:"epoch,omitempty"`
	Seq         *int64     `json:"seq,omitempty"`
}
type HelloInfo struct {
	Protocol    int64    `json:"protocol"`
	MinProtocol int64    `json:"minProtocol"`
	Features    []string `json:"features"`
	ServerTime  Ms       `json:"serverTime"`
}
type HelloRequestAmesh struct {
	Version string `json:"version"`
	OS      string `json:"os"`
	Arch    string `json:"arch"`
}
type HelloRequestClaudeCode struct {
	Versions     []string `json:"versions"`
	PeerProtocol int64    `json:"peerProtocol"`
	NativeCheck  string   `json:"nativeCheck"`
}
type HelloRequest struct {
	ResumeEpoch  *Epoch                 `json:"resumeEpoch"`
	Takeover     bool                   `json:"takeover"`
	Amesh        HelloRequestAmesh      `json:"amesh"`
	ClaudeCode   HelloRequestClaudeCode `json:"claudeCode"`
	Capabilities []string               `json:"capabilities"`
}
type Settings struct {
	HeartbeatMs         int64 `json:"heartbeatMs"`
	StreamSilenceMs     int64 `json:"streamSilenceMs"`
	BackoffInitialMs    int64 `json:"backoffInitialMs"`
	BackoffMaxMs        int64 `json:"backoffMaxMs"`
	BackoffResetAfterMs int64 `json:"backoffResetAfterMs"`
	AcceptWindowMs      int64 `json:"acceptWindowMs"`
	RequestTimeoutMs    int64 `json:"requestTimeoutMs"`
	SendWaitMs          int64 `json:"sendWaitMs"`
}
type LimitsMachineRate struct {
	Limit    int64 `json:"limit"`
	WindowMs int64 `json:"windowMs"`
}
type Limits struct {
	MaxTextBytes    int64             `json:"maxTextBytes"`
	HopLimit        int64             `json:"hopLimit"`
	QueueTTLMs      int64             `json:"queueTtlMs"`
	OfflineQueueMax int64             `json:"offlineQueueMax"`
	MachineRate     LimitsMachineRate `json:"machineRate"`
	KeepaliveMs     int64             `json:"keepaliveMs"`
	OfflineAfterMs  int64             `json:"offlineAfterMs"`
}
type HelloResponseMachine struct {
	ID   MachineID   `json:"id"`
	Name MachineName `json:"name"`
}
type HelloResponse struct {
	Protocol    int64                `json:"protocol"`
	MinProtocol int64                `json:"minProtocol"`
	Machine     HelloResponseMachine `json:"machine"`
	Epoch       Epoch                `json:"epoch"`
	Features    []string             `json:"features"`
	Settings    Settings             `json:"settings"`
	Limits      Limits               `json:"limits"`
	ServerTime  Ms                   `json:"serverTime"`
}
type SessionRegisterRequest struct {
	Epoch      Epoch  `json:"epoch"`
	SessionID  string `json:"sessionId"`
	Name       string `json:"name"`
	NameSource string `json:"nameSource"`
	Project    string `json:"project"`
	Status     string `json:"status"`
	Entrypoint string `json:"entrypoint"`
	ClaudeCode string `json:"claudeCode"`
	StartedAt  Ms     `json:"startedAt"`
}
type SessionInfo struct {
	Key          SessionKey  `json:"key"`
	Address      Address     `json:"address"`
	Name         string      `json:"name"`
	Project      string      `json:"project"`
	Machine      MachineName `json:"machine"`
	Status       string      `json:"status"`
	RegisteredAt Ms          `json:"registeredAt"`
}
type SessionRegisterResponse struct {
	Session SessionInfo `json:"session"`
}
type HeartbeatRequestSessionsItem struct {
	Key    SessionKey `json:"key"`
	Status string     `json:"status"`
}
type HeartbeatRequest struct {
	Epoch    Epoch                          `json:"epoch"`
	Cursor   int64                          `json:"cursor"`
	Sessions []HeartbeatRequestSessionsItem `json:"sessions"`
}
type HeartbeatResponse struct {
	Unknown    []SessionKey `json:"unknown"`
	Ended      []SessionKey `json:"ended"`
	ServerTime Ms           `json:"serverTime"`
}
type HumanTurnRequest struct {
	Epoch  Epoch  `json:"epoch"`
	At     Ms     `json:"at"`
	Source string `json:"source"`
}
type HumanTurnResponse struct {
	Reset int64 `json:"reset"`
}
type SessionEndRequest struct {
	Epoch  Epoch  `json:"epoch"`
	Reason string `json:"reason"`
}
type SessionEndResponseSession struct {
	Key    SessionKey `json:"key"`
	Status string     `json:"status"`
}
type SessionEndResponse struct {
	Session SessionEndResponseSession `json:"session"`
	Dropped int64                     `json:"dropped"`
}
type Peer struct {
	Address  Address      `json:"address"`
	Kind     Kind         `json:"kind"`
	Name     string       `json:"name"`
	Machine  *MachineName `json:"machine"`
	Project  *string      `json:"project"`
	Status   PeerStatus   `json:"status"`
	LastSeen *Ms          `json:"lastSeen"`
}
type PeersResponse struct {
	Peers      []Peer `json:"peers"`
	ServerTime Ms     `json:"serverTime"`
}
type SendRequestFrom struct {
	Session SessionKey `json:"session"`
}
type SendRequestTo struct {
	Address  *AddressInput `json:"address,omitempty"`
	Endpoint *EndpointID   `json:"endpoint,omitempty"`
}
type SendRequest struct {
	Epoch          Epoch           `json:"epoch"`
	ID             UUID            `json:"id"`
	From           SendRequestFrom `json:"from"`
	To             SendRequestTo   `json:"to"`
	Text           string          `json:"text"`
	HopChain       HopChain        `json:"hopChain"`
	FromMode       FromMode        `json:"fromMode"`
	Origin         string          `json:"origin"`
	NotifyWhenIdle bool            `json:"notifyWhenIdle"`
	FailureNotice  bool            `json:"failureNotice"`
}
type EndpointRef struct {
	ID   EndpointID `json:"id"`
	Name string     `json:"name"`
	Peer Address    `json:"peer"`
}
type SendResponseMessageTo struct {
	Address Address    `json:"address"`
	Kind    Kind       `json:"kind"`
	Status  PeerStatus `json:"status"`
}
type SendResponseMessage struct {
	ID        UUID                  `json:"id"`
	State     DeliveryState         `json:"state"`
	To        SendResponseMessageTo `json:"to"`
	Endpoint  EndpointRef           `json:"endpoint"`
	Hop       Hop                   `json:"hop"`
	CreatedAt Ms                    `json:"createdAt"`
	ExpiresAt Ms                    `json:"expiresAt"`
}
type SendResponse struct {
	Message SendResponseMessage `json:"message"`
}
type DeliveryState = string
type DropReason = string
type StateReport struct {
	Kind    string      `json:"kind"`
	ID      UUID        `json:"id"`
	Session SessionKey  `json:"session"`
	State   string      `json:"state"`
	Reason  *DropReason `json:"reason"`
	At      Ms          `json:"at"`
}
type IdleReport struct {
	Kind    string     `json:"kind"`
	ID      UUID       `json:"id"`
	Session SessionKey `json:"session"`
	At      Ms         `json:"at"`
}
type Report = json.RawMessage
type ReportsRequest struct {
	Epoch   Epoch    `json:"epoch"`
	Reports []Report `json:"reports"`
}
type ReportsResponseResultsItem struct {
	ID      UUID          `json:"id"`
	State   DeliveryState `json:"state"`
	Applied bool          `json:"applied"`
}
type ReportsResponse struct {
	Results []ReportsResponseResultsItem `json:"results"`
}
type EndpointEventPeer struct {
	Address Address `json:"address"`
	Kind    Kind    `json:"kind"`
}
type EndpointEvent struct {
	Kind      string            `json:"kind"`
	Seq       int64             `json:"seq"`
	ID        EndpointID        `json:"id"`
	Name      string            `json:"name"`
	Peer      EndpointEventPeer `json:"peer"`
	Context   *string           `json:"context"`
	Sessions  []SessionKey      `json:"sessions"`
	State     string            `json:"state"`
	CreatedAt Ms                `json:"createdAt"`
	ClosedAt  *Ms               `json:"closedAt"`
}
type MessageEvent struct {
	Kind           string      `json:"kind"`
	Seq            int64       `json:"seq"`
	ID             UUID        `json:"id"`
	Session        SessionKey  `json:"session"`
	Endpoint       EndpointRef `json:"endpoint"`
	Text           string      `json:"text"`
	HopChain       HopChain    `json:"hopChain"`
	FromMode       *FromMode   `json:"fromMode"`
	Hop            Hop         `json:"hop"`
	NotifyWhenIdle bool        `json:"notifyWhenIdle"`
	FailureNotice  bool        `json:"failureNotice"`
	CreatedAt      Ms          `json:"createdAt"`
	ExpiresAt      Ms          `json:"expiresAt"`
}
type DeliveryEventTo struct {
	Address  Address    `json:"address"`
	Endpoint EndpointID `json:"endpoint"`
}
type DeliveryEvent struct {
	Kind    string          `json:"kind"`
	Seq     int64           `json:"seq"`
	ID      UUID            `json:"id"`
	Session SessionKey      `json:"session"`
	To      DeliveryEventTo `json:"to"`
	State   DeliveryState   `json:"state"`
	Reason  *string         `json:"reason"`
	Message string          `json:"message"`
	At      Ms              `json:"at"`
}
type LimitEventStopped struct {
	ID        UUID   `json:"id"`
	Direction string `json:"direction"`
}
type LimitEvent struct {
	Kind     string            `json:"kind"`
	Seq      int64             `json:"seq"`
	Session  SessionKey        `json:"session"`
	Endpoint EndpointRef       `json:"endpoint"`
	Hop      Hop               `json:"hop"`
	Stopped  LimitEventStopped `json:"stopped"`
	Message  string            `json:"message"`
	At       Ms                `json:"at"`
}
type IdleEvent struct {
	Kind    string     `json:"kind"`
	Seq     int64      `json:"seq"`
	ID      UUID       `json:"id"`
	Session SessionKey `json:"session"`
	Peer    Address    `json:"peer"`
	At      Ms         `json:"at"`
}
type StatusEvent struct {
	Kind   string `json:"kind"`
	Peer   Peer   `json:"peer"`
	Listed bool   `json:"listed"`
}
type SynchronizedEvent struct {
	Kind string `json:"kind"`
	Seq  int64  `json:"seq"`
}
type RevokedEvent struct {
	Kind    string    `json:"kind"`
	Machine MachineID `json:"machine"`
	Message string    `json:"message"`
	At      Ms        `json:"at"`
}
type SupersededEvent struct {
	Kind    string `json:"kind"`
	Epoch   Epoch  `json:"epoch"`
	By      Epoch  `json:"by"`
	Message string `json:"message"`
	At      Ms     `json:"at"`
}
type PeerEvent = json.RawMessage
type EventStreamItem struct {
	Event string  `json:"event"`
	Data  string  `json:"data"`
	ID    *string `json:"id,omitempty"`
	Retry *int64  `json:"retry,omitempty"`
}
type NativeUserFrameMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}
type NativeUserFrame struct {
	Type     string                 `json:"type"`
	Message  NativeUserFrameMessage `json:"message"`
	Priority string                 `json:"priority"`
	From     NativeAddress          `json:"from"`
	MsgV     int64                  `json:"msgV"`
	MsgID    UUID                   `json:"msg_id"`
}
type NativeAddress = string
type NativeNotifyWhenIdle struct {
	Type     string        `json:"type"`
	Action   string        `json:"action"`
	From     NativeAddress `json:"from"`
	MsgID    UUID          `json:"msg_id"`
	MsgV     int64         `json:"msgV"`
	FromMode FromMode      `json:"from_mode"`
}
type NativeReceipt struct {
	Type          string        `json:"type"`
	Action        string        `json:"action"`
	Status        string        `json:"status"`
	StatusDetail  *string       `json:"status_detail,omitempty"`
	Reason        string        `json:"reason"`
	From          NativeAddress `json:"from"`
	OrigMsgID     UUID          `json:"orig_msg_id"`
	DropReason    *string       `json:"drop_reason,omitempty"`
	DroppedMsgIDs *[]UUID       `json:"dropped_msg_ids,omitempty"`
	MsgV          int64         `json:"msgV"`
	MsgID         UUID          `json:"msg_id"`
}
type NativeIdleNotice struct {
	Type       string        `json:"type"`
	Action     string        `json:"action"`
	OrigMsgID  UUID          `json:"orig_msg_id"`
	State      string        `json:"state"`
	FinishedAt Ms            `json:"finished_at"`
	From       NativeAddress `json:"from"`
	FromMode   FromMode      `json:"from_mode"`
	MsgV       int64         `json:"msgV"`
	MsgID      UUID          `json:"msg_id"`
}
type HookInputUserPromptSubmit struct {
	HookEventName  string  `json:"hook_event_name"`
	SessionID      string  `json:"session_id"`
	TranscriptPath *string `json:"transcript_path,omitempty"`
	Cwd            *string `json:"cwd,omitempty"`
	PromptID       *string `json:"prompt_id,omitempty"`
	PermissionMode *string `json:"permission_mode,omitempty"`
	Prompt         string  `json:"prompt"`
	SessionTitle   *string `json:"session_title,omitempty"`
}
type HookInputPreToolUse struct {
	HookEventName  string          `json:"hook_event_name"`
	SessionID      string          `json:"session_id"`
	Cwd            *string         `json:"cwd,omitempty"`
	PermissionMode *string         `json:"permission_mode,omitempty"`
	AgentID        *string         `json:"agent_id,omitempty"`
	AgentType      *string         `json:"agent_type,omitempty"`
	ToolName       string          `json:"tool_name"`
	ToolInput      *map[string]any `json:"tool_input,omitempty"`
	ToolUseID      *string         `json:"tool_use_id,omitempty"`
}
type HookInput = json.RawMessage
type HookOutputPreToolUseHookSpecificOutput struct {
	HookEventName            string `json:"hookEventName"`
	PermissionDecision       string `json:"permissionDecision"`
	PermissionDecisionReason string `json:"permissionDecisionReason"`
}
type HookOutputPreToolUse struct {
	HookSpecificOutput HookOutputPreToolUseHookSpecificOutput `json:"hookSpecificOutput"`
}
type NotHumanPromptPrefixes = []string
type LocalError struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}
type DaemonStatusDaemon struct {
	Version   string `json:"version"`
	PID       int64  `json:"pid"`
	StartedAt Ms     `json:"startedAt"`
}
type DaemonStatusMachine struct {
	ID   MachineID   `json:"id"`
	Name MachineName `json:"name"`
}
type DaemonStatusHub struct {
	State  string     `json:"state"`
	Since  Ms         `json:"since"`
	Epoch  *Epoch     `json:"epoch"`
	Cursor int64      `json:"cursor"`
	Halt   *ErrorBody `json:"halt"`
}
type DaemonStatusConfigDirsItem struct {
	Path     string `json:"path"`
	Sessions int64  `json:"sessions"`
}
type DaemonStatusSessionsItem struct {
	Key     SessionKey `json:"key"`
	Address Address    `json:"address"`
	Status  PeerStatus `json:"status"`
	Role    string     `json:"role"`
}
type DaemonStatusLanesItemParent struct {
	Name      string `json:"name"`
	SessionID string `json:"sessionId"`
}
type DaemonStatusLanesItemChild struct {
	Name      string `json:"name"`
	SessionID string `json:"sessionId"`
	ConfigDir string `json:"configDir"`
}
type DaemonStatusLanesItem struct {
	ID     LaneID                      `json:"id"`
	Relay  NativeAddress               `json:"relay"`
	Parent DaemonStatusLanesItemParent `json:"parent"`
	Child  DaemonStatusLanesItemChild  `json:"child"`
}
type DaemonStatusOutbox struct {
	Count    int64 `json:"count"`
	OldestAt *Ms   `json:"oldestAt"`
}
type DaemonStatusOpen struct {
	Count int64 `json:"count"`
}
type DaemonStatus struct {
	Daemon     DaemonStatusDaemon           `json:"daemon"`
	Machine    *DaemonStatusMachine         `json:"machine"`
	Hub        DaemonStatusHub              `json:"hub"`
	ConfigDirs []DaemonStatusConfigDirsItem `json:"configDirs"`
	Sessions   []DaemonStatusSessionsItem   `json:"sessions"`
	Lanes      []DaemonStatusLanesItem      `json:"lanes"`
	Outbox     DaemonStatusOutbox           `json:"outbox"`
	Open       DaemonStatusOpen             `json:"open"`
}
type LaneID = string
type McpListPeersArguments struct {
	Filter *string `json:"filter,omitempty"`
}
type McpSendMessageArguments struct {
	To             string `json:"to"`
	Message        string `json:"message"`
	NotifyWhenIdle *bool  `json:"notify_when_idle,omitempty"`
}
type McpSendToParentArguments struct {
	Message string `json:"message"`
}
type McpListPeersResultYou struct {
	Name    Address     `json:"name"`
	Project string      `json:"project"`
	Machine MachineName `json:"machine"`
}
type McpListPeersResultPeersItem struct {
	Name     Address    `json:"name"`
	Kind     Kind       `json:"kind"`
	Machine  *string    `json:"machine"`
	Project  *string    `json:"project"`
	Status   PeerStatus `json:"status"`
	LastSeen *string    `json:"last_seen"`
}
type McpListPeersResult struct {
	You   McpListPeersResultYou         `json:"you"`
	Peers []McpListPeersResultPeersItem `json:"peers"`
	Stale *string                       `json:"stale,omitempty"`
}
type McpSendResult struct {
	Accepted bool       `json:"accepted"`
	To       *Address   `json:"to,omitempty"`
	ID       *UUID      `json:"id,omitempty"`
	Note     *string    `json:"note,omitempty"`
	Reason   *string    `json:"reason,omitempty"`
	Known    *[]Address `json:"known,omitempty"`
}
type McpSendToParentResult struct {
	Accepted bool    `json:"accepted"`
	To       *string `json:"to,omitempty"`
	ID       *UUID   `json:"id,omitempty"`
	Reason   *string `json:"reason,omitempty"`
}
type McpCallToolResultContentItem struct {
	Type string `json:"type"`
	Text string `json:"text"`
}
type McpCallToolResult struct {
	Content           []McpCallToolResultContentItem `json:"content"`
	StructuredContent map[string]any                 `json:"structuredContent"`
	IsError           bool                           `json:"isError"`
}
type TestKey struct {
	Note               *string `json:"note,omitempty"`
	PrivateKeyPKCS8PEM string  `json:"privateKeyPkcs8Pem"`
	PublicKeySPKI      string  `json:"publicKeySpki"`
}
type MachineIDVector struct {
	Rule          string    `json:"rule"`
	PublicKeySPKI string    `json:"publicKeySpki"`
	MachineID     MachineID `json:"machineId"`
}
type SessionKeyVector struct {
	Rule       string     `json:"rule"`
	PID        int64      `json:"pid"`
	ProcStart  string     `json:"procStart"`
	SessionKey SessionKey `json:"sessionKey"`
}
type RelaySocketVector struct {
	Rule       string        `json:"rule"`
	MachineID  MachineID     `json:"machineId"`
	EndpointID EndpointID    `json:"endpointId"`
	Path       string        `json:"path"`
	Address    NativeAddress `json:"address"`
}
type LaneSocketVector struct {
	Rule           string    `json:"rule"`
	ParentArgument string    `json:"parentArgument"`
	ChildSessionID string    `json:"childSessionId"`
	MachineID      MachineID `json:"machineId"`
	LaneID         LaneID    `json:"laneId"`
	Path           string    `json:"path"`
}
type SignatureVector struct {
	Name           string    `json:"name"`
	Protocol       int64     `json:"protocol"`
	Method         string    `json:"method"`
	Target         string    `json:"target"`
	Body           string    `json:"body"`
	Ts             int64     `json:"ts"`
	Nonce          string    `json:"nonce"`
	MachineID      MachineID `json:"machineId"`
	BodyHash       string    `json:"bodyHash"`
	Canonical      string    `json:"canonical"`
	Signature      string    `json:"signature"`
	Authorization  string    `json:"authorization"`
	SignatureValid bool      `json:"signatureValid"`
	Note           *string   `json:"note,omitempty"`
}
type EnvelopeInput struct {
	Body        string         `json:"body"`
	From        *NativeAddress `json:"from,omitempty"`
	FromSession *string        `json:"fromSession,omitempty"`
	HopChain    *HopChain      `json:"hopChain,omitempty"`
	FromName    *string        `json:"fromName,omitempty"`
	FromMode    *FromMode      `json:"fromMode,omitempty"`
}
type EnvelopeVector struct {
	Name       string        `json:"name"`
	Input      EnvelopeInput `json:"input"`
	Content    string        `json:"content"`
	Attributed bool          `json:"attributed"`
	Note       *string       `json:"note,omitempty"`
}

func DefaultSettings() Settings {
	return Settings{HeartbeatMs: 20000, StreamSilenceMs: 45000, BackoffInitialMs: 500, BackoffMaxMs: 10000, BackoffResetAfterMs: 5000, AcceptWindowMs: 5000, RequestTimeoutMs: 10000, SendWaitMs: 5000}
}
func (s Settings) Valid() bool {
	return s.HeartbeatMs >= 5000 && s.HeartbeatMs <= 30000 &&
		s.StreamSilenceMs >= 20000 && s.StreamSilenceMs <= 120000 &&
		s.BackoffInitialMs >= 100 && s.BackoffInitialMs <= 5000 &&
		s.BackoffMaxMs >= 1000 && s.BackoffMaxMs <= 60000 &&
		s.BackoffResetAfterMs >= 1000 && s.BackoffResetAfterMs <= 60000 &&
		s.AcceptWindowMs >= 1000 && s.AcceptWindowMs <= 30000 &&
		s.RequestTimeoutMs >= 2000 && s.RequestTimeoutMs <= 60000 &&
		s.SendWaitMs >= 0 && s.SendWaitMs <= 15000
}
