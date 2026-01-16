package treeptik.ldallen.controllers.sockjs;

import com.github.aesteve.vertx.nubes.annotations.sockjs.bridge.EventBusBridge;
import com.github.aesteve.vertx.nubes.annotations.sockjs.bridge.InboundPermitted;
import com.github.aesteve.vertx.nubes.annotations.sockjs.bridge.OutboundPermitted;
import com.github.aesteve.vertx.nubes.annotations.sockjs.bridge.events.*;
import io.vertx.core.eventbus.EventBus;
import io.vertx.ext.web.handler.sockjs.BridgeEvent;

@EventBusBridge("/eventbus/*")
@OutboundPermitted(address = "chat.to.client")
@InboundPermitted(address = "chat.to.server")
public class EBBridgeController {

	@SOCKET_CREATED
	public void onSocketCreated(BridgeEvent be, EventBus eb) {
		forward(be, eb);
	}

	@SOCKET_CLOSED
	public void onSocketClosed(BridgeEvent be, EventBus eb) {
		forward(be, eb);
	}

	@SEND
	public void onClientMsgSent(BridgeEvent be, EventBus eb) {
		forward(be, eb);
	}

	@PUBLISH
	public void onPublish(BridgeEvent be, EventBus eb) {
		forward(be, eb);
	}

	@RECEIVE
	public void onReceive(BridgeEvent be, EventBus eb) {
		forward(be, eb);
	}

	@REGISTER
	public void onRegister(BridgeEvent be, EventBus eb) {
		forward(be, eb);
	}

	private void forward(BridgeEvent be, EventBus eb) {
		String type = be.type().toString();
		eb.send(type, type);
		be.complete(true);
	}

}