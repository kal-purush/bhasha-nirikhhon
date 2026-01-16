import { SlackBot } from 'botkit';

export const init: (err: string, bot: SlackBot) => void = (err, bot) => {
  if (err) {
    throw new Error('Could not connect to Slack');
  }
  bot.say({
    channel: 'front-end',
    text: 'Good morning :sunny: \n Please tell me the progress ? :eyes:',
    username: 'sushi-kun',
    icon_url: 'https://ca.slack-edge.com/T30J6REKB-UDF5GG831-b05e0e33b5d8-72',
  });
};