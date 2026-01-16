package com.soa.rs.discordbot.v3.commands;

import java.util.ArrayList;
import java.util.List;

import com.soa.rs.discordbot.v3.api.annotation.Interaction;
import com.soa.rs.discordbot.v3.api.command.AbstractCommand;
import com.soa.rs.discordbot.v3.jdbi.SettingsUtility;

import discord4j.common.util.Snowflake;
import discord4j.core.event.domain.interaction.ButtonInteractionEvent;
import discord4j.core.event.domain.interaction.ChatInputInteractionEvent;
import discord4j.core.event.domain.interaction.ModalSubmitInteractionEvent;
import discord4j.core.event.domain.message.MessageCreateEvent;
import discord4j.core.object.command.ApplicationCommandInteractionOption;
import discord4j.core.object.command.ApplicationCommandInteractionOptionValue;
import discord4j.core.object.component.ActionRow;
import discord4j.core.object.component.Button;
import discord4j.core.object.component.LayoutComponent;
import discord4j.core.object.component.TextInput;
import discord4j.core.object.entity.PartialMember;
import discord4j.core.object.entity.User;
import discord4j.core.object.entity.channel.Channel;
import discord4j.core.object.entity.channel.GuildMessageChannel;
import discord4j.core.object.reaction.ReactionEmoji;
import discord4j.core.spec.EmbedCreateSpec;
import discord4j.core.spec.MessageCreateSpec;
import discord4j.rest.util.Color;
import reactor.core.publisher.Mono;

@Interaction(trigger = "rollcall")
public class RollcallInteraction extends AbstractCommand {

	private final SettingsUtility settingsUtility = new SettingsUtility();
	private final String ROLLCALL_SETTING_PREFIX = "rollcall-channel-";

	@Override
	public void initialize() {
		setEnabled(true);
	}

	@Override
	public Mono<Void> execute(MessageCreateEvent event) {
		return Mono.empty();
	}

	@Override
	public Mono<Void> execute(ChatInputInteractionEvent event) {
		if (event.getInteraction().getGuildId().isEmpty()) {
			return event.reply("This command may only be used in a guild.").withEphemeral(true).then();
		}

		Mono<Channel> rollCallChannel = event.getOption("rollcall_channel")
				.flatMap(ApplicationCommandInteractionOption::getValue)
				.map(ApplicationCommandInteractionOptionValue::asChannel).orElse(Mono.empty());

		Mono<Channel> rollCallResponsesChannel = event.getOption("rollcall_responses_channel")
				.flatMap(ApplicationCommandInteractionOption::getValue)
				.map(ApplicationCommandInteractionOptionValue::asChannel).orElse(Mono.empty());

		EmbedCreateSpec spec = EmbedCreateSpec.create();
		spec = spec.withTitle("Respond to the Roll Call!");
		spec = spec.withDescription("Thank you for reading the above message from clan leadership.\n\n"
				+ "To let us know that you have read the message and receive attendance credit, click the button below "
				+ "and fill in the form to let us know that you've been Elfed!");
		spec = spec.withColor(Color.of(19, 114, 38));

		Button responseButton = Button.success("rollcall-" + event.getInteraction().getGuildId().get().asString() + "-" + event.getClient().getSelfId().asString(),
				ReactionEmoji.codepoints("U+1F9DD"), "Respond to the Roll Call!");

		EmbedCreateSpec finalSpec = spec;
		return event.deferReply().withEphemeral(true).then(Mono.fromRunnable(
						() -> settingsUtility.insertOrUpdateValueForKey(
								ROLLCALL_SETTING_PREFIX + event.getInteraction().getGuildId().get().asString(),
								rollCallResponsesChannel.map(channel -> channel.getId().asString()).block())))
				.then(rollCallChannel.ofType(GuildMessageChannel.class).flatMap(
						guildMessageChannel -> guildMessageChannel.createMessage(
								MessageCreateSpec.builder().addEmbed(finalSpec)
										.addComponent(ActionRow.of(responseButton)).build())))
				.then(event.createFollowup("Rollcall created.").withEphemeral(true)).then();
	}

	@Override
	public Mono<Void> execute(ModalSubmitInteractionEvent event) {
		TextInput rollCallResponse = event.getComponents(TextInput.class).get(0);

		return event.deferReply().withEphemeral(true).then(Mono.fromCallable(() -> settingsUtility.getValueForKey(
						ROLLCALL_SETTING_PREFIX + event.getInteraction().getGuildId().get().asString()))
				.flatMap(id -> event.getClient().getChannelById(Snowflake.of(id)).ofType(GuildMessageChannel.class))
				.flatMap(guildMessageChannel -> guildMessageChannel.createMessage(
						event.getInteraction().getUser().asMember(event.getInteraction().getGuildId().get())
								.map(User::getMention).block() + " (" + event.getInteraction().getUser()
								.asMember(event.getInteraction().getGuildId().get()).map(PartialMember::getDisplayName)
								.block() + ") responded to the Roll Call with: `" + rollCallResponse.getValue().get() + "`"))
				.then(event.createFollowup(
								"Thank you for reading and responding to our roll call!  Your response has been recorded.")
						.withEphemeral(true))).then();
	}

	@Override
	public Mono<Void> execute(ButtonInteractionEvent event) {
		List<LayoutComponent> modalComponents = new ArrayList<>();
		modalComponents.add(ActionRow.of(TextInput.small("rollcall-text-response", "Roll Call Response", 1, 500)
				.placeholder("Your roll call response here, e.g. 'I've been Elfed!").required()));

		return event.presentModal("SoA Roll Call Response!",
				"rollcall-modal-" + event.getInteraction().getGuildId().get().asString() + "-" + event.getClient().getSelfId().asString(), modalComponents).then();
	}
}