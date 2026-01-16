import type ApplicationCommand from './ApplicationCommand'
import config from '../config'

const command: ApplicationCommand = {
  meta: {
    name: 'legal',
    description: 'Legal information about the bot and its services',
    options: []
  },
  execute: async interaction => {
    await interaction.reply({
      embeds: [
        {
          title: 'Legal Information',
          description:
            `By using this bot, you accept PBDB’s [Terms of Service](${config.officialRepository}/blob/master/docs/terms.md) ` +
            `and [Privacy Policy](${config.officialRepository}/blob/master/docs/privacy.md).`,
          color: config.nonJesusColor
        }
      ],
      ephemeral: true
    })
  }
}

export default command