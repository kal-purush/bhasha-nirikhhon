import Discord from 'discord.js';
import { readdir as rd } from 'fs';
import { promisify } from 'util';
import { UsersDb } from './users-db';
import { randInt } from './utils';
const readdir = promisify(rd);

interface ICommand {
  name: string;
  description: string;
  usage?: string;
  group: string;
  requiredPermissions?: string[];
  guildOnly: boolean;
  aliases?: string[];
  execute(message: Discord.Message, commandArgs: string[]): void;
}

export interface IPotrBotConfig {
  prefix: string;
  postgresDbUri: string;
  discordBotToken: string;
}

export class PotrBot {
  private client: Discord.Client;
  private db: UsersDb;
  private commands: Discord.Collection<string, ICommand>;

  constructor(private config: IPotrBotConfig) {
    this.client = new Discord.Client();
    this.db = new UsersDb(this.config.postgresDbUri, console.error);
    this.commands = new Discord.Collection();

    this.client.once('ready', this.onceReady);
    this.client.on('message', this.onMessage);
  }

  public start() {
    this.client.login(this.config.discordBotToken);
  }

  private async onceReady() {
    console.log(`Logging in with ${this.client.user.username}#${this.client.user.discriminator}`);

    // Setup database
    await this.db.initialize();

    try {
      this.commands = await this.loadAllCommands('commands', ['general', 'moderation']);
    } catch (err) {
      console.error(`Unable to load groups. Error: ${err}`);
    }
  }

  private async onMessage(message: Discord.Message) {
    // If command is not of valid format or is written by a bot, return
    if (message.author.bot) {
      return;
    }

    if (message.channel.type === 'text') {
      const randomExpAmount = randInt(20, 30);
      if (await this.db.userExists(message.guild.id, message.author.id)) {
        await this.db.increaseUserExp(randomExpAmount, message.guild.id, message.author.id);
      } else {
        await this.db.createUser(message.guild.id, message.author.id, randomExpAmount);
      }

      const { exp, level } = (await this.db.getUserRows(message.guild.id, message.author.id))
        .rows[0] as any;

      // 100 exp required for each level. Will be changed later
      if (exp > level * 100) {
        await Promise.all([
          this.db.increaseUserLevel(message.guild.id, message.author.id),
          message.channel.send(
            `Congratulations ${message.author}! You've leveled up to level ${level + 1}`,
          ),
        ]);
      }
    }

    if (
      !message.content.startsWith(this.config.prefix) ||
      message.content.trim().length === this.config.prefix.length
    ) {
      return;
    }

    const commandArgs = message.content.slice(this.config.prefix.length).split(/ +/);
    const commandName = commandArgs.shift() || '';

    // Try to get command by primary name, otherwise check aliases
    const command =
      this.commands.get(commandName) ||
      this.commands.find(
        cmd => cmd.hasOwnProperty('aliases') && cmd.aliases!.includes(commandName),
      );

    // If command/aliases not found, return
    if (!command) {
      return;
    }

    // Filter commands with wrong number of arguments
    if (command.usage) {
      const numberOfArgs = command.usage
        .split(' ')
        .filter(arg => arg.startsWith('<') && arg.endsWith('>'));
      if (commandArgs.length < numberOfArgs.length) {
        await message.channel.send(
          `The command \`${commandName}\` expects ${numberOfArgs.length - commandArgs.length}` +
            `more argument(s)!\n` +
            `The correct usage would be \`${this.config.prefix}${commandName} ${command.usage}\``,
        );
        return;
      }
    }

    // Filter guild-only commands
    if (command.guildOnly && message.channel.type !== 'text') {
      await message.channel.send("I can't use that command in DMs!");
      return;
    }

    // Check if user has required permissions to run the command
    if (
      command.requiredPermissions &&
      !message.member.hasPermission(command.requiredPermissions as Discord.PermissionResolvable)
    ) {
      await message.channel.send("You don't have the required permissions to run this command!");
      return;
    }

    try {
      await command.execute(message, commandArgs);
    } catch (err) {
      // TODO: Better error handling
      await message.channel.send('An error occurred while running this command!');
      console.error(`An error occurred! ${err}`);
    }
  }

  private async loadAllCommands(commandsDir: string, commandGroups: string[]) {
    const result = new Discord.Collection<string, ICommand>();

    // For all directories in `commandGroups` (with `commandsDir` as root),
    // store all commands in `result`.
    for (const group of commandGroups) {
      try {
        const commandFiles = (await readdir(`${__dirname}/${commandsDir}/${group}`)).filter(file =>
          file.endsWith('.js'),
        );

        for (const file of commandFiles) {
          const commandObject: ICommand = require(`${__dirname}/${commandsDir}/${group}/${file}`);
          result.set(commandObject.name, commandObject);
        }
      } catch (err) {
        console.error(`Unable to load group ${group}. Error: ${err}`);
      }
    }

    return result;
  }
}