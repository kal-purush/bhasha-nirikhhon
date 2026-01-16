import { EmbedBuilder, PermissionFlagsBits, SlashCommandBuilder } from "discord.js";
import type { Interaction } from "discord.js";
import type { Command } from "../../../types/impl/discord";
import { getByGuildId } from "../../../database/impl/tables/guilds/impl/get";
import { colors } from "../..";

export default {
    data: new SlashCommandBuilder().setName("get-guild").setDescription("Fetches the current guild and displays the data.").setDefaultMemberPermissions(PermissionFlagsBits.ManageRoles),
    execute: async (interaction: Interaction) => {
        if (!interaction.isCommand()) return;

        const guild = await getByGuildId({
            guild_id: interaction.guildId ?? "",
        });

        if (!guild) {
            const embed = new EmbedBuilder().setDescription("The guild does not exist.").setColor(colors.errorColor);
            await interaction.reply({ embeds: [embed], ephemeral: true });
            return;
        }

        const discordGuild = await interaction.guild?.fetch();

        const embed = new EmbedBuilder()
            .setTitle(`Guild Info: ${discordGuild?.name}`)
            .setColor(colors.baseColor)
            .addFields(
                { name: "Guild ID", value: `\`${guild.guild_id}\``, inline: true },
                { name: "Created At", value: `${guild.created_at.toISOString()}`, inline: true },
                { name: "Channels", value: `${guild.channels.length.toString()}`, inline: true },
                { name: "Roles", value: `${guild.roles.length.toString()}`, inline: true },
                { name: "Users", value: `${guild.users.length.toString()}`, inline: true },
            );

        // Add detailed channel information
        guild.channels.forEach((channel, index) => {
            embed.addFields({
                name: `Channel ${index + 1}: #${channel.metadata.name}`,
                value: `Type: \`${channel.type}\`\nPosition: \`${channel.metadata.position ?? "N/A"}\`\nNSFW: \`${channel.metadata.nsfw ? "Yes" : "No"}\``,
                inline: false,
            });
        });

        guild.roles.forEach((role, index) => {
            embed.addFields({
                name: `Role ${index + 1}: \`${role.metadata.name}\``,
                value: `Permissions: \`${role.metadata.permissions.length > 0 ? role.metadata.permissions.join(", ") : "None"}\`\nColor: \`${role.metadata.color.toString(16)}\`\nHoist: \`${role.metadata.hoist ? "Yes" : "No"}\``,
                inline: false,
            });
        });

        await interaction.reply({ embeds: [embed], ephemeral: false });
    },
} as Command;