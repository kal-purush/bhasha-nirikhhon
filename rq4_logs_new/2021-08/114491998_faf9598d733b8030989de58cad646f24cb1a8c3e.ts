import {
	ButtonInteraction,
	CommandInteraction,
	GuildMember,
	InteractionReplyOptions,
	MessageActionRow,
	MessageButton,
} from "discord.js";
import { CrafterRole, RaiderRole, removeRoleSelectedMessage, removeRoleSelectMessage } from "../config";
import { DiscordRole } from "../typings";

export const removeRoleCommand = "removerole";

/**
 * Gather the available roles for the user in the interaction, and return a button for each.
 * @param interaction The interaction containing the user
 * @returns The list of available buttons for the user's roles. It can potentially be empty
 */
const getAppliedRoleButtons = (interaction: CommandInteraction): MessageButton[] => {
	const guildMember = interaction.member as GuildMember;
	const buttons: MessageButton[] = [];
	const serverRaiderRole = interaction.guild.roles.cache.get(RaiderRole.id);
	if (serverRaiderRole && guildMember.roles.cache.has(serverRaiderRole.id)) {
		const raiderButton: MessageButton = new MessageButton()
			.setCustomId(RaiderRole.key)
			.setLabel(RaiderRole.name)
			.setStyle("DANGER");
		buttons.push(raiderButton);
	}

	const serverCrafterRole = interaction.guild.roles.cache.get(CrafterRole.id);
	if (serverCrafterRole && guildMember.roles.cache.has(serverCrafterRole.id)) {
		const crafterButton: MessageButton = new MessageButton()
			.setCustomId(CrafterRole.key)
			.setLabel(CrafterRole.name)
			.setStyle("DANGER");
		buttons.push(crafterButton);
	}

	return buttons;
};

/**
 * Remove the role from the interaction from the user in the interaction.
 * @param interaction THe user interaction from the button press
 */
const removeRole = async (interaction: ButtonInteraction): Promise<void> => {
	const { member } = interaction;
	const guildMember = member as GuildMember;

	let role: DiscordRole;
	if (interaction.customId === RaiderRole.key) {
		role = RaiderRole;
	} else if (interaction.customId === CrafterRole.key) {
		role = CrafterRole;
	}

	if (!role) {
		await interaction.update("Something has gone wrong!");
		return;
	}

	const serverRole = interaction.guild.roles.cache.get(role.id);
	if (guildMember.roles.cache.has(serverRole.id)) {
		guildMember.roles.remove(serverRole.id);
	}

	await interaction.update({ content: removeRoleSelectedMessage });
};

/**
 * Handle remove role slash commands, removing the role to the user if it does not exist.
 * @param interaction The user interaction from the slash command
 */
export const handleRemoveRoleCommand = async (interaction: CommandInteraction): Promise<void> => {
	const roleButtons = getAppliedRoleButtons(interaction);
	if (roleButtons.length === 0) {
		await interaction.reply("You don't have any removable roles!");
		return;
	}

	const messageAction = new MessageActionRow().addComponents(roleButtons);
	const messageOptions: InteractionReplyOptions = {
		content: removeRoleSelectMessage,
		ephemeral: true,
		components: [messageAction],
	};
	await interaction.reply(messageOptions);

	const { member } = interaction;
	const removeRoleResponseFilter = (i) => i.user.id === member.user.id;
	const roleCollector = interaction.channel.createMessageComponentCollector({
		filter: removeRoleResponseFilter,
		time: 15000,
	});
	roleCollector.on("collect", removeRole);
};