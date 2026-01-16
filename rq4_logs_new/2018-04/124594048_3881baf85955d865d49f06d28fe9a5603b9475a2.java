package it.cbmz.raspo.internal.core;

import it.cbmz.raspo.internal.util.Constants;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.event.Event;
import org.osgi.service.event.EventAdmin;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Component(
	immediate = true,
	property = {
		"scheduler.period:Long=30",
		"scheduler.concurrent:Boolean=false",
		"scheduler.name=ping"
	}
)
public class PingScheduler implements Runnable {

	@Override
	public void run() {

		Map<String, Object> map = new HashMap<>(2, 2);

		map.put("command", Constants.Command.PING);

		map.put("topic", Constants.Handler.SEND_HANDLER);

		_eventAdmin.postEvent(
			new Event(Constants.Handler.START_SPEED_TEST_HANDLER, map));

	}

	@Reference
	private EventAdmin _eventAdmin;

}