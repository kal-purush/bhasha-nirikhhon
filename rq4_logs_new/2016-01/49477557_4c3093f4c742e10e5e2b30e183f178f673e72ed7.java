package ws.prager.models;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class Node {
	
	final static Logger logger = LoggerFactory.getLogger(Node.class);
	private long startTime;

	public Node() {
		logger.debug("creating node bean");
		startTime = java.lang.System.currentTimeMillis();
	}

	public long getStartTime() {
		return startTime;
	}

}