import { Embed, EmbedBuilder, Interaction } from "discord.js";
import { get as getOperator } from "../../../lib/impl/operators/impl/get";
import { getByCharId, getModuleDetails } from "../../../lib/impl/modules/impl/get";
import type { Menu } from "../../../types/impl/discord";
import { colors } from "../..";
import { insertBlackboard } from "../../../lib/impl/operators/impl/helper";

export default {
    id: "get-operator",
    execute: async (interaction: Interaction) => {
        if (!interaction.isAnySelectMenu()) return;

        await interaction.deferUpdate();

        const operatorId = interaction.customId.split(":")[1];
        const type = interaction.customId.split(":")[2];
        const moduleLevel = Number(interaction.values[0]);

        const operator = await getOperator(operatorId);

        const embeds: Embed[] = [];
        if (interaction.message.embeds[0]) embeds.push(interaction.message.embeds[0]);

        const newEmbeds = [];

        switch (type) {
            case "modules":
                const operatorModules = await getByCharId(operator?.id ?? "");
                if (operatorModules.length === 0) {
                    const embed = new EmbedBuilder().setDescription("No modules found.").setColor(colors.errorColor);
                    newEmbeds.push(embed);
                    break;
                }

                for (const module of operatorModules) {
                    const moduleData = await getModuleDetails(module.uniEquipId);
                    if (!isNaN(moduleLevel)) {
                        const phase = moduleData?.phases.find((phase) => phase.equipLevel === moduleLevel);
                        if (!phase) continue;
                        const embed = new EmbedBuilder().setTitle(`${module.typeIcon.toUpperCase()} ${module.uniEquipName} - Lv${phase.equipLevel}`);
                        if ((module.image ?? "").length > 0) embed.setThumbnail(module.image!);

                        let description = "",
                            talentName = null,
                            talentDescription = null;
                        for (const part of phase.parts) {
                            if (part.overrideTraitDataBundle.candidates) {
                                const candidate = part.overrideTraitDataBundle.candidates[part.overrideTraitDataBundle.candidates.length - 1];
                                if (candidate.additionalDescription) {
                                    description += `${insertBlackboard(candidate.additionalDescription, candidate.blackboard)}\n`;
                                }
                                if (candidate.overrideDescripton) {
                                    description += `${insertBlackboard(candidate.overrideDescripton, candidate.blackboard)}\n`;
                                }
                            }
                            if (part.addOrOverrideTalentDataBundle.candidates) {
                                const candidate = part.addOrOverrideTalentDataBundle.candidates[part.addOrOverrideTalentDataBundle.candidates.length - 1];
                                talentName = candidate.name ?? talentName;
                                talentDescription = insertBlackboard(candidate.upgradeDescription, candidate.blackboard) ?? talentDescription;
                            }
                        }
                        embed.setDescription(description);
                        if (talentName && talentDescription) {
                            embed.addFields({ name: talentName, value: talentDescription });
                        }

                        const statDescription = phase.attributeBlackboard.map((attribute) => `${attribute.key.toUpperCase()} ${attribute.value > 0 ? "+" : ""}${attribute.value}`).join("\n");
                        embed.addFields({ name: `Stats`, value: statDescription });

                        newEmbeds.push(embed);
                    }
                }
                break;
            default:
                break;
        }

        await interaction.message.edit({ embeds: [...embeds, ...newEmbeds] });
    },
} as Menu;