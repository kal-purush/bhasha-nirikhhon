package treeptik.ldallen.controller.sockjs;

import com.github.aesteve.vertx.nubes.annotations.sockjs.bridge.EventBusBridge;
import com.github.aesteve.vertx.nubes.annotations.sockjs.bridge.InboundPermitted;
import com.github.aesteve.vertx.nubes.annotations.sockjs.bridge.OutboundPermitted;

@EventBusBridge("/eventbus/*")
@OutboundPermitted(address = "service.tasks")
@OutboundPermitted(address = "web.tasks.service")
@InboundPermitted(address = "service.tasks")
@InboundPermitted(address = "web.tasks.service")
public class EBBridgeController {

}