import { SlashCommandBuilder } from "discord.js";
import type { Interaction } from "discord.js";
import type { Command } from "../../../types/impl/discord";

export default {
    data: new SlashCommandBuilder().setName("ping").setDescription("Dadadada!"),
    execute: async (interaction: Interaction) => {
        if (!interaction.isCommand()) return;

        const responses = ["Dadadada!", "Gooooooo!", "Haah!", "Prey should stay still!"];
        await interaction.reply(responses[Math.floor(Math.random() * responses.length)]);
    },
} as Command;