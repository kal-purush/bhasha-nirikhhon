import fs from 'fs'
import path from 'path'
import discord from 'discord.js'
import type ApplicationCommand from '../ApplicationCommand'
import config from '../../config'

const termFiles = fs.readdirSync(__dirname)

function loadTermsFromFiles (): Record<string, { title: string, definition: string }> {
  const terms: Record<string, { title: string, definition: string }> = {}

  for (const file of termFiles) {
    if (!file.endsWith('.md')) continue

    const { yamlString, definition } = fs.readFileSync(path.join(__dirname, file), 'utf-8').match(/---\n(?<yamlString>.+?)\n---\n(?<definition>.+)/s)?.groups ?? { yamlString: '', definition: '' }
    const title = yamlString?.match(/title: (?<title>.+)/)?.groups?.['title'] ?? ''
    const id = file.replace(/\.md$/, '')
    terms[id] = { title, definition: definition ?? '' }
  }

  return terms
}

const terms = loadTermsFromFiles()

const command: ApplicationCommand = {
  meta: {
    name: 'term',
    description: 'Look up a theological term.',
    options: [{
      name: 'term',
      description: 'The theological term to look up.',
      type: discord.ApplicationCommandOptionType.String,
      required: true,
      choices: Object.entries(terms).map(([id, { title }]) => ({ name: title, value: id }))
    }]
  },
  execute: async interaction => {
    const term = interaction.options.getString('term', true)
    const { title, definition } = terms[term] ?? { title: '', definition: '' }

    if (title === '') {
      await interaction.reply({
        embeds: [{
          title: 'Term Not Found',
          description: `The term **${term}** was not found in the database.`
        }],
        ephemeral: true
      })
      return
    }

    await interaction.reply({
      embeds: [{
        title,
        description: definition,
        color: config.nonJesusColor
      }]
    })
  }
}

export default command