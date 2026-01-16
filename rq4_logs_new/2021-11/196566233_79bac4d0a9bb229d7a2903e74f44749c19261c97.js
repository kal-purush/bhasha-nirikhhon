const baninfo = require("../lib/banInfo");
const config = require("../config.json");
const { SlashCommandBuilder } = require("@discordjs/builders");

module.exports = {
	data: new SlashCommandBuilder()
		.setName("baninfo")
		.setDescription("Informa sobre la situación de un usuario")
		.addUserOption(option =>
			option
				.setName("jugador")
				.setDescription("Jugador al que se desea saber su situación")
				.setRequired(false)
		),
	channels: [config.channelid, config.botschannelid],
	async execute(interaction) {
		if (!interaction.options.getUser("jugador")) {
			try {
				await baninfo(interaction, interaction.user);
			} catch (e) {
				throw new Error(e);
			}
		} else {
			try {
				await baninfo(interaction, interaction.options.getUser("jugador"));
			} catch (e) {
				throw new Error(e);
			}
		}
	}
};