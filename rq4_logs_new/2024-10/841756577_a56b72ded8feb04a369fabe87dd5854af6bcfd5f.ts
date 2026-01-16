import { EmbedBuilder, SlashCommandBuilder } from "discord.js";
import type { Interaction } from "discord.js";
import type { Command } from "../../../types/impl/discord";
import { search } from "../../../lib/impl/operators/impl/search";
import { get } from "../../../lib/impl/operators/impl/get";
import { colors } from "../..";
import { OperatorRarity } from "../../../types/impl/lib/impl/operators";

export default {
    data: new SlashCommandBuilder()
        .setName("get-operator")
        .setDescription("Get's an Arknights operator and displays their data.")
        .addStringOption((option) => option.setName("operator").setDescription("The operator to get data for.").setRequired(true).setAutocomplete(true)),
    execute: async (interaction: Interaction) => {
        if (!interaction.isCommand()) return;

        const operatorId = interaction.options.get("operator")?.value as string;
        const operator = await get(operatorId);

        if (!operator) {
            const embed = new EmbedBuilder().setDescription("Operator not found.").setColor(colors.errorColor);

            return await interaction.reply({ embeds: [embed], ephemeral: true });
        }

        const skinImage =
            operator.rarity === OperatorRarity.oneStar || operator.rarity === OperatorRarity.twoStar || operator.rarity === OperatorRarity.threeStar
                ? `https://raw.githubusercontent.com/Aceship/Arknight-Images/main/characters/${encodeURIComponent(operator.id?.replaceAll("#", "_") ?? "")}_1.png`
                : `https://raw.githubusercontent.com/Aceship/Arknight-Images/main/characters/${encodeURIComponent(operator.id?.replaceAll("#", "_") ?? "")}_2.png`;
        const talents = Array.isArray(operator.talents)
            ? operator.talents.map((talent) => `${Array.isArray(talent.candidates) ? talent.candidates.map((c) => `**${c.name}**: ${c.description.replace(/<[^>]*>/g, "").replace(/{[^}]*}/g, "")} - \`${c.unlockCondition?.phase} Lvl ${c.unlockCondition?.level}\``).join("\n") : "None"}`).join("\n")
            : "None";
        const skills = Array.isArray(operator.skills) && operator.skills.length > 0 ? operator.skills.map((skill) => `**${skill.static?.levels[0]?.name || "Unknown"}**: ${skill.static?.levels[0]?.description.replace(/<[^>]*>/g, "").replace(/{[^}]*}/g, "")}`).join("\n") : "None";

        const embed = new EmbedBuilder()
            .setTitle(`${operator.name} ${operator.appellation && operator.appellation.length > 1 ? `(${operator.appellation})` : ""}`)
            .setDescription(operator.description.replace(/<[^>]*>/g, "").replace(/{[^}]*}/g, ""))
            .setColor(operator.isSpChar ? 0x9b59b6 : 0x3498db) // Purple for special chars, blue for others
            .setThumbnail(`https://raw.githubusercontent.com/yuanyan3060/ArknightsGameResource/main/avatar/${operator.id}.png`) // Replace with real CDN path
            .setImage(skinImage) // Replace with real CDN path
            .addFields(
                { name: "Profession", value: operator.profession ?? "", inline: true },
                { name: "Position", value: `${operator.position ?? ""}`, inline: true },
                { name: "Rarity", value: operator.rarity ?? "", inline: true },
                { name: "Nation", value: operator.nationId ?? "", inline: true },
                { name: "Group", value: operator.groupId ?? "N/A", inline: true },
                { name: "Tags", value: Array.isArray(operator.tagList) ? operator.tagList.join(", ") : "None", inline: false },
                {
                    name: "Is Obtainable?",
                    value: operator.isNotObtainable ? "No" : "Yes",
                    inline: true,
                },
                {
                    name: "Potential Items",
                    value: `General: ${operator.canUseGeneralPotentialItem ? "✅" : "❌"}\n` + `Activity: ${operator.canUseActivityPotentialItem ? "✅" : "❌"}`,
                    inline: true,
                },
                { name: "Max Potential Level", value: `${operator.maxPotentialLevel ?? ""}`, inline: true },
                {
                    name: "Talent Info",
                    value: talents.length >= 1024 ? talents.substring(0, 1020) + "..." : talents,
                    inline: false,
                },
                {
                    name: "Phases",
                    value: Array.isArray(operator.phases)
                        ? operator.phases.map((phase) => `Max Level: \`${phase.maxLevel}\`\nElite Cost: ${Array.isArray(phase.evolveCost) && phase.evolveCost !== null ? phase.evolveCost.map((item) => `\`${item.count}x ${item.type}\``).join(", ") : "`N/A`"}`).join("\n\n")
                        : "None",
                    inline: false,
                },
                {
                    name: "Skills",
                    value: skills.length >= 1024 ? skills.substring(0, 1020) + "..." : skills,
                    inline: false,
                },
            )
            .setFooter({ text: `ID: ${operator.id || "N/A"} | Display Number: ${operator.displayNumber}` });

        await interaction.reply({ embeds: [embed] });
    },
    autocomplete: async (interaction: Interaction) => {
        if (interaction.isAutocomplete()) {
            const focusedValue = interaction.options.getFocused().toLowerCase();
            const operators = await search(focusedValue);

            await interaction.respond(
                operators.slice(0, 25).map((choice) => ({
                    name: choice.name,
                    value: choice.id ?? "",
                })),
            );
        }
    },
} as Command;