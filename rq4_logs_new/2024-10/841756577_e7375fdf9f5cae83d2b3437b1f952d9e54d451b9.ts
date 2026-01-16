import { EmbedBuilder, SlashCommandBuilder } from "discord.js";
import type { GuildMember, Interaction } from "discord.js";
import type { Command } from "../../../types/impl/discord";
import { search as searchOperators } from "../../../lib/impl/operators/impl/search";
import { search as searchVoiceLines } from "../../../lib/impl/voices/impl/search";
import { get as getOperator } from "../../../lib/impl/operators/impl/get";
import { getByCharacterId as getVoice } from "../../../lib/impl/voices/impl/get";
import { colors } from "../..";
import { joinVC } from "../voice/impl/joinVC";
import { playAudio } from "../voice/impl/playAudio";

export default {
    data: new SlashCommandBuilder()
        .setName("voice")
        .setDescription("Plays the voice line of an operator.")
        .addStringOption((option) => option.setName("operator").setDescription("The operator to play the voice line for.").setRequired(true).setAutocomplete(true))
        .addStringOption((option) => option.setName("voice").setDescription("The voice line to play.").setRequired(true).setAutocomplete(true)),
    execute: async (interaction: Interaction) => {
        if (!interaction.isCommand()) return;

        await interaction.deferReply();

        const operatorId = interaction.options.get("operator")?.value as string;
        const voiceTitle = interaction.options.get("voice")?.value as string;

        const operator = await getOperator(operatorId);
        const voiceLines = await getVoice(operatorId);
        const voice = voiceLines.find((voice) => voice.voiceTitle === voiceTitle);

        if (!operator) {
            const embed = new EmbedBuilder().setDescription("The operator you provided does not exist.").setColor(colors.errorColor);
            return await interaction.editReply({ embeds: [embed] });
        }
        if (!voice) {
            const embed = new EmbedBuilder().setDescription("The voice line you provided does not exist.").setColor(colors.errorColor);
            return await interaction.editReply({ embeds: [embed] });
        }

        if (!(interaction.member as GuildMember)!.voice?.channelId) {
            const embed = new EmbedBuilder().setDescription("You must be in a voice channel to use this command.").setColor(colors.errorColor);
            return await interaction.editReply({ embeds: [embed] });
        }

        const connection = await joinVC(interaction.client, (interaction.member as GuildMember)!.voice.channelId!);
        if (!connection) {
            const embed = new EmbedBuilder().setDescription("Failed to join the voice channel.").setColor(colors.errorColor);
            return await interaction.editReply({ embeds: [embed] });
        }

        const embed = new EmbedBuilder()
            .setTitle(voice?.voiceTitle ?? "")
            .setDescription(`**Operator:** ${operator?.name}\n**Voice Line:** ${voice?.voiceText}\n**Voice URL:** [Click here](${voice?.voiceURL})`)
            .setColor(colors.successColor)
            .setTimestamp();

        await interaction.editReply({ embeds: [embed] });

        await playAudio(voice.voiceURL ?? "", connection);
    },
    autocomplete: async (interaction: Interaction) => {
        if (interaction.isAutocomplete()) {
            const choiceName = interaction.options.getFocused(true).name;
            if (choiceName === "operator") {
                const operators = await searchOperators(interaction.options.getFocused().toLowerCase());
                return await interaction.respond(
                    operators.slice(0, 25).map((choice) => ({
                        name: choice.name,
                        value: choice.id ?? "",
                    })),
                );
            } else if (choiceName === "voice") {
                const voiceLines = await searchVoiceLines(interaction.options.getFocused().toLowerCase());
                return await interaction.respond(
                    voiceLines.slice(0, 25).map((choice) => ({
                        name: choice.name,
                        value: choice.name ?? "",
                    })),
                );
            }
        }
    },
} as Command;