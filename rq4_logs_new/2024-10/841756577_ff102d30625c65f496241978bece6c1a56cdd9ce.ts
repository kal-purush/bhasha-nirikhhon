import { EmbedBuilder, SlashCommandBuilder } from "discord.js";
import type { Interaction } from "discord.js";
import type { Command } from "../../../types/impl/discord";
import { getByDiscordId as getUser } from "../../../database/impl/tables/users/impl/get";
import { colors } from "../..";

export default {
    data: new SlashCommandBuilder()
        .setName("get-user")
        .setDescription("Get's an users experience and level.")
        .addUserOption((option) => option.setName("target").setDescription("The user to get the experience and level of.")),
    execute: async (interaction: Interaction) => {
        if (!interaction.isCommand()) return;

        const user = interaction.options.get("target")?.user ?? interaction.user;

        const data = await getUser({
            user_id: user.id,
        });

        if (!data) {
            const embed = new EmbedBuilder().setDescription("User not found.").setColor(colors.errorColor);
            return await interaction.reply({ embeds: [embed], ephemeral: true });
        }

        const embed = new EmbedBuilder()
            .setTitle(`User: ${user.tag}`)
            .setDescription(`Level: ${data.level}\nExperience: ${data.exp}\nFavorite Operator: ${data.favorites.operator ?? "None"}`)
            .setColor(colors.successColor);
        await interaction.reply({ embeds: [embed] });
    },
} as Command;