package somepackage;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Some third party service.
 *
 */
@Component
@Profile("prod")
public class DefaultThirdPartyGateway implements ThirdPartyGateway {

	private final SomeThirdPartyClient serviceClient;

	private static final Logger logger = LoggerFactory.getLogger(DefaultThirdPartyGateway.class);

	@Autowired
	public DefaultThirdPartyGateway(SomeThirdPartyClient client) {
		this.serviceClient = client;
	}

	@Override
	public Map<String, Object> method1(String param1) {
		logger.info("invoking third party client for param {}", param1);
		return serviceClient.getData(param1);
	}


}