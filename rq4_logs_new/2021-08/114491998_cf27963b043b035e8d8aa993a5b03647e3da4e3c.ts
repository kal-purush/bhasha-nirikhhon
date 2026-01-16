import { Interaction } from "discord.js";
import { handleSlashCommands as handleRoleAssignmentSlashCommands, slashCommands } from "../../plugins/role-assigner";

const roleAssignerCommands = slashCommands.map((cmd) => cmd.name);

/**
 * Handler function for interactions with the bot.
 * @param interaction The interaction from the user
 */
export async function onInteraction(interaction: Interaction): Promise<void> {
	if (!interaction.isCommand()) {
		return;
	}

	if (roleAssignerCommands.includes(interaction.commandName)) {
		await handleRoleAssignmentSlashCommands(interaction);
	}
}