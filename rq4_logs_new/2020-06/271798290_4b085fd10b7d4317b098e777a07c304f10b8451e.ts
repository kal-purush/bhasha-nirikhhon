import dotenv from 'dotenv';

dotenv.config();

import Discord from 'discord.js';
import { Match, Player, PlayerMap } from './types';
import { CallbackTest } from './test';

const prefix = process.env.BOT_PREFIX || 'm!';

const idPlayerMapper = new PlayerMap();
let currentMatch: Match | null = null;

let test: CallbackTest | null = null;

const client: Discord.Client = new Discord.Client({
  retryLimit: 3,
});

client.on('message', async (message: Discord.Message) => {
  if (message.content.startsWith(prefix)) {
    // m!vote @
    const cmds: string[] = message.content
      .slice(prefix.length)
      .trim()
      .split(/\s+/);

    if (!(message.channel instanceof Discord.TextChannel)) return;

    const currentPlayer = idPlayerMapper.addOrFindPlayer(message.author);
    switch (cmds[0]) {
      case 'create':
        if (currentMatch) message.channel.send('There is an ongoing match');
        message.channel.send('Creating a match');
        currentMatch = new Match(message.channel.id, currentPlayer);
        break;
      case 'cancel':
        if (!currentMatch) message.channel.send('There is no ongoing match');
        else if (currentMatch.ongoingGame())
          message.channel.send('There is an ongoing game');
        else if (!currentMatch.isLeader(currentPlayer))
          message.channel.send('User is not the party leader');
        else {
          currentMatch = null;
          message.channel.send('Match has been disbanded');
        }
        break;
      case 'start':
      case 'remake':
        if (!currentMatch) {
          message.channel.send('There is no ongoing match');
          return;
        }
        try {
          const game = currentMatch.startGame();

          const m = game.getMafia();
          m.discordUser().send('You are the mafia');
          const ps = game.getOthers();
          ps.forEach((it) => {
            it.discordUser().send('You are not the mafia');
          });

          const teams = game.getUserNameList();

          const embed = new Discord.MessageEmbed();

          embed.setTitle('Game on!');
          embed.setDescription(
            'A game has been started. All players have received the DM!'
          );
          embed.addFields([
            { name: 'Team A', value: teams[0].join('\n'), inline: true },
            { name: 'Team B', value: teams[1].join('\n'), inline: true },
          ]);
          message.channel.send(embed);
        } catch (e) {
          if (e instanceof Error) message.channel.send(e.message);
        }
        break;
      case 'join':
        if (!currentMatch) {
          message.channel.send('There is no ongoing match');
          return;
        }
        try {
          currentMatch.addPlayer(currentPlayer);
        } catch (e) {
          if (e instanceof Error) message.channel.send(e.message);
        }
        break;
      case 'leave':
        if (!currentMatch) {
          message.channel.send('There is no ongoing match');
          return;
        }
        try {
          currentMatch.removePlayer(currentPlayer);
        } catch (e) {
          if (e instanceof Error) message.channel.send(e.message);
        }
        break;
      case 'win':
        if (
          !cmds[1] ||
          cmds[1].toLowerCase() !== 'a' ||
          cmds[1].toLowerCase() !== 'b'
        )
          message.channel.send(`Usage: \`${prefix}win <team A/B>\``);
        else if (!currentMatch) {
          message.channel.send('There is no ongoing match');
        } else {
          const winnerIndex = cmds[1].toLowerCase() === 'a' ? 0 : 1;
          try {
            currentMatch.setGameWinner(winnerIndex);
            currentMatch.startVote((result) => {
              message.channel.send('test');
            });
            const players = currentMatch.getPlayers();
            const embed = new Discord.MessageEmbed();
            embed.setTitle('Vote for mafia');
            embed.setDescription(
              'Voting ends in 60 seconds or all players have voted'
            );
            embed.addField('Players', players.join('\n'));
            await message.channel.send(embed);

            // logic:
            // either 60 seconds timer is expired
            // or all players have voted

            let counter = 0;
            const interval = setInterval(() => {
              if (counter === 3) clearInterval(interval);
              message.channel.send(
                `You have ${60 - counter * 15} seconds left`
              );
              counter++;
            }, 15 * 1000);

            await sleep(60 * 1000);
            await message.channel.send('Voting is over');

            currentMatch.endGame();
          } catch (e) {
            if (e instanceof Error) message.channel.send(e.message);
          }
        }
        break;
      case 'vote':
        if (!cmds[1]) {
          message.channel.send(`Usage: \`${prefix}vote <@player>\``);
        } else if (!currentMatch) {
          message.channel.send('There is no ongoing match');
        } else {
          try {
            // currentMatch.playerVote(currentPlayer);
            console.log('test');
          } catch (e) {
            if (e instanceof Error) message.channel.send(e.message);
          }
        }
        break;
      case 'commands':
      case 'help':
        const embed = new Discord.MessageEmbed();
        const allcmds = [
          'create',
          'cancel',
          'join',
          'leave',
          'win <team>',
          'remake',
          `commands (alias: ${prefix}help)`,
        ];
        embed.setTitle('Commands list');
        embed.addField(
          'Commands',
          allcmds.map((it) => `${prefix}${it}`).join('\n')
        );
        message.channel.send(embed);
        break;
      // case 'embedtest':
      //   const embed = new Discord.MessageEmbed();
      //   embed.setTitle('Game on!');
      //   embed.setDescription(
      //     'A game has been started. All players have received the DM!'
      //   );
      //   embed.addFields([
      //     { name: 'Team A', value: 'Seeeeeeee\nB\nC', inline: true },
      //     { name: 'Team B', value: 'T\nA\nUeeeeeeee', inline: true },
      //   ]);
      //   message.channel.send(embed).then(async (m) => {
      //     const emotes = ['1️⃣', '2️⃣', '3️⃣', '4️⃣', '5️⃣', '6️⃣'];
      //     emotes.map(async (e) => await m.react(e));
      //   });
      //   break;
      // case 'dmtest':
      //   currentPlayer.discordUser().send('Test DM');
      case 'callbacktest':
        test = new CallbackTest();

        let counter = 0;
        message.channel.send(`You have 20 seconds left`);
        const interval = setInterval(() => {
          counter++;
          if (counter === 3) clearInterval(interval);
          message.channel.send(`You have ${20 - counter * 5} seconds left`);
        }, 5 * 1000);

        test.setCallback(() => {
          clearInterval(interval);
          message.channel.send('Callback received');
        });

        await sleep(20 * 1000);
        test.doCallback();
        break;
      case 'forcecallback':
        if (!test) return;
        test.forceCallback();
        break;
      default:
        return;
    }
  }
});

function sleep(ms: number) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

client.login(process.env.BOT_TOKEN!);