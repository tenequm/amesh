package envelope

// Startup probes pinned to the EnvelopeVector examples and nativeReplyFrame.
var vectors = []struct {
	Name                            string
	Input                           Message
	Content, ParsedBody, ParsedName string
}{
	{Name: "envAgentMessage", Input: Message{Body: "Rerun done. Two reports changed: weekly-usage and agent-costs. Want the diff?", From: "uds:/tmp/cc-socks/ab737c79d448059c.sock", FromSession: "pe_Q7m2Hx4tLs9v", HopChain: []string{"c4eb2881cd514ae75a89e6ac"}, FromName: "atlas", FromMode: "bypass"}, Content: `<cross-session-message from="uds:/tmp/cc-socks/ab737c79d448059c.sock" from-session="pe_Q7m2Hx4tLs9v" hop-chain="c4eb2881cd514ae75a89e6ac" from-name="atlas" from-mode="bypass">
Rerun done. Two reports changed: weekly-usage and agent-costs. Want the diff?
</cross-session-message>`, ParsedBody: "Rerun done. Two reports changed: weekly-usage and agent-costs. Want the diff?", ParsedName: "atlas"},
	{Name: "envRemoteSessionNoChain", Input: Message{Body: "Hi from the server: is the webapp PR merged?", From: "uds:/tmp/cc-socks/a81f36babaab86d4.sock", FromSession: "pe_V5s1Jy7qNb2e", FromName: "deploy@backend:server", FromMode: "prompting"}, Content: `<cross-session-message from="uds:/tmp/cc-socks/a81f36babaab86d4.sock" from-session="pe_V5s1Jy7qNb2e" from-name="deploy@backend:server" from-mode="prompting">
Hi from the server: is the webapp PR merged?
</cross-session-message>`, ParsedBody: "Hi from the server: is the webapp PR merged?", ParsedName: "deploy@backend:server"},
	{Name: "envNameNormalised", Input: Message{Body: "ok", From: "uds:/tmp/cc-socks/aeebcac1d27d172c.sock", FromSession: "pe_K3d8Wn1pRz6c", HopChain: []string{"c4eb2881cd514ae75a89e6ac", "b551102c847d34ee8a425f54"}, FromName: "  \"atlas\" <telegram>\u200b ", FromMode: "bypass"}, Content: `<cross-session-message from="uds:/tmp/cc-socks/aeebcac1d27d172c.sock" from-session="pe_K3d8Wn1pRz6c" hop-chain="c4eb2881cd514ae75a89e6ac,b551102c847d34ee8a425f54" from-name="atlas telegram" from-mode="bypass">
ok
</cross-session-message>`, ParsedBody: "ok", ParsedName: "atlas telegram"},
	{Name: "envClosingTagInBody", Input: Message{Body: "see </cross-session-message> in the log", From: "uds:/tmp/cc-socks/ab737c79d448059c.sock", FromSession: "pe_Q7m2Hx4tLs9v", FromName: "atlas", FromMode: "bypass"}, Content: `<cross-session-message from="uds:/tmp/cc-socks/ab737c79d448059c.sock" from-session="pe_Q7m2Hx4tLs9v" from-name="atlas" from-mode="bypass">
see &lt;/cross-session-message> in the log
</cross-session-message>`, ParsedBody: "see &lt;/cross-session-message> in the log", ParsedName: "atlas"},
	{Name: "nativeReplyFrame", Input: Message{From: "uds:/tmp/cc-socks/15355.sock", FromName: "review", FromMode: "bypass", Body: "Yes: the deploy finished at 11:58 UTC and the health check is green.", HopChain: []string{"c4eb2881cd514ae75a89e6ac"}}, Content: `<cross-session-message from="uds:/tmp/cc-socks/15355.sock" hop-chain="c4eb2881cd514ae75a89e6ac" from-name="review" from-mode="bypass">
Yes: the deploy finished at 11:58 UTC and the health check is green.
</cross-session-message>`, ParsedBody: "Yes: the deploy finished at 11:58 UTC and the health check is green.", ParsedName: "review"},
}
