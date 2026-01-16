import { EmbedBuilder, PermissionFlagsBits, SlashCommandBuilder } from "discord.js";
import type { Interaction } from "discord.js";
import type { Command } from "../../../types/impl/discord";

export default {
    data: new SlashCommandBuilder().setName("create-guide").setDescription("Guide on how to use the /create-guild command.").setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels),
    execute: async (interaction: Interaction) => {
        if (!interaction.isCommand()) return;

        const embed = new EmbedBuilder()
            .setTitle("Guide on how to use the /create-guild command.")
            .setDescription("To create a new guild, use the /create-guild command with the name option.")
            .addFields([
                {
                    name: "channels",
                    value: "The channels should be an array of channel JSON. To fetch the channel JSON, you can use the /get-channel command.",
                    inline: true,
                },
                {
                    name: "users",
                    value: "The users should be an array of user JSON. To fetch the user JSON, you can use the /get-user command.",
                    inline: true,
                },
                {
                    name: "roles",
                    value: "The roles should be an array of role JSON. To fetch the role JSON, you can use the /get-role command.",
                    inline: true,
                },
            ]);
        await interaction.reply({ embeds: [embed], ephemeral: true });
    },
} as Command;