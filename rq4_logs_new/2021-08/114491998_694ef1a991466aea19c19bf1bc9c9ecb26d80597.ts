import { CommandInteraction, MessageEmbed } from "discord.js";

/**
 * The role command key.
 */
export const setupRaidCommand = "setupraid";

// eslint-disable-next-line @typescript-eslint/no-unused-vars
const reactionFilter = (_: any, __: any) => {
	console.log("APPLYING FILTER");
	return true;
};

const handleReaction = async (reactionInteraction): Promise<void> => {
	console.log(reactionInteraction);
};

export const handleSetupRaidCommand = async (interaction: CommandInteraction): Promise<void> => {
	const embed = new MessageEmbed()
		.setTitle(interaction.options.getString("title"))
		.setDescription(`Raid Leader: ${interaction.member.user.username}`)
		.setColor("RED")
		.addFields(
			{
				name: "Open Slots",
				value: interaction.options.getInteger("participants").toString(),
			},
			{
				name: "Date",
				value: interaction.options.getString("date"),
				inline: true,
			},
			{
				name: "Time",
				value: interaction.options.getString("time"),
				inline: true,
			}
		);

	await interaction.reply("Setting up your raid!");

	const reply = await interaction.channel.send({ embeds: [embed] });
	console.log("GOT HERE");
	const collector = reply.createReactionCollector({ filter: reactionFilter, time: 15000000 });
	collector.on("collect", (reactionInteraction) => handleReaction(reactionInteraction));
};