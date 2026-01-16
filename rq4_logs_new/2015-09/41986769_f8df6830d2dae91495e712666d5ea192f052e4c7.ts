import * as net from "../index";

net.isIP('0.0.0.0');

net.isIPv4('0.0.0.0');

net.isIPv6('::');

net.connect(8080, 'https://github.com', _ => {
	
});