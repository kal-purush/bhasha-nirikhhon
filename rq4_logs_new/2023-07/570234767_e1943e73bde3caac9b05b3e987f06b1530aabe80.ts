import {
    CommandInteraction, EmbedBuilder,
    EmbedBuilder as EmbedBuilderType,
    GuildMember, ModalSubmitInteraction,
    UserContextMenuCommandInteraction
} from "discord.js";
import {Bank, Player} from "@prisma/client";

export default {
    createGurubankEmbed: (interaction: UserContextMenuCommandInteraction | CommandInteraction | ModalSubmitInteraction, player: Player & {
        coins: Bank | null
    }, target: GuildMember | undefined = undefined, edit: boolean = false): EmbedBuilderType => {
        const member = interaction.member as GuildMember;
        const embed = new EmbedBuilder();
        if (edit) {
            embed
                .setThumbnail(target ? target.displayAvatarURL() : member.displayAvatarURL())
                .setTitle(`Gurubank de ${target ? target.displayName : member.displayName}`)
                .setDescription(target ? `${member}, voici le contenu de la nouvelle \`\`Gurubank\`\` de ${target}.` : `${member}, voici le contenu de votre \`\`Gurubank\`\`.`)
        } else {
            embed
                .setThumbnail(target ? target.displayAvatarURL() : member.displayAvatarURL())
                .setTitle(`Gurubank de ${target ? target.displayName : member.displayName}`)
                .setDescription(target ? `${member}, voici le contenu de la \`\`Gurubank\`\` de ${target}.` : `${member}, voici le contenu de votre \`\`Gurubank\`\`.`)
        }

        embed
            .setColor(0x8636B1)
            .addFields({
                    name: 'Pain Coins',
                    value: `${player.coins?.painCoin ?? 0} <:paincoin:873563604992016485>`,
                    inline: true
                },
                {
                    name: 'Agony Coins',
                    value: `${player.coins?.agonyCoin ?? 0} <:agonycoin:873566300608299029>`,
                    inline: true
                },
                {
                    name: 'Despair Coins',
                    value: `${player.coins?.despairCoin ?? 0} <:despaircoin:873568372112105523>`,
                    inline: true
                },
            )
            .setFooter({
                text: `Demandé par ${member.displayName ?? "N/A"}`,
                iconURL: member.avatarURL() ?? undefined
            })
            .setTimestamp(new Date());

        return embed;
    }
}