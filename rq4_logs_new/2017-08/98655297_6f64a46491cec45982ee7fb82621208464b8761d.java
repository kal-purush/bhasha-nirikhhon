package ircapi;

import java.io.IOException;

import org.pircbotx.Configuration;
import org.pircbotx.PircBotX;
import org.pircbotx.exception.IrcException;
import org.pircbotx.hooks.ListenerAdapter;
import org.pircbotx.hooks.types.GenericMessageEvent;

public class AgentListener extends ListenerAdapter {

	@Override
	public void onGenericMessage(GenericMessageEvent event) {
		final String msg = event.getMessage();
		System.out.println(msg);
		final String response = parse(msg);
		if (response != null) {
			event.getBot().getUserChannelDao().getChannel("#designpatternsdiscoclub").send().message(response);
		}
	}

	public String parse(String msg) {
		if (msg.startsWith("beat/")) {
			final String[] args = msg.split("/");
			switch (args[2]) {
			case "1":
				return "sampler/128Drums";
			case "2":
				return "sampler/hh2";
			case "3":
				return "sampler/vox5";
			case "4":
				return "sampler/scratch";
			default:
				break;
			}
			return "sampler/vox1";
		}
		return null;
	}

	public static void main(String[] args) throws Exception {
		startIrcServer();
	}

	public static void startIrcServer() throws IOException, IrcException {
		// may use a web client like https://kiwiirc.com to test IRC
		final String botNiickname = "MyAgent";
		final String networkName = "irc.foonetic.net";
		final String channelName = "#designpatternsdiscoclub";
		Configuration configuration = new Configuration.Builder().setName(botNiickname).addServer(networkName)
				.addAutoJoinChannel(channelName).addListener(new AgentListener()).buildConfiguration();
		final PircBotX bot = new PircBotX(configuration);
		bot.startBot();
	}
}