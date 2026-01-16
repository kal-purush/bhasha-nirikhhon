import { WebClient } from "@slack/web-api";
import dotenv from "dotenv";

// Load .env file.
dotenv.config();

const bot = new WebClient(process.env.BOT_TOKEN);
const user = new WebClient(process.env.USER_TOKEN);

const MAX_MESSAGE_NUMBER = 50;

interface JointedChannel {
  id: string;
  created: string;
}

(async () => {
  // Bot が参加しているチャンネル一覧.
  const joinedChannels: JointedChannel[] = await bot.channels
    .list()
    .then(res => {
      return (res.channels as any[])
        .filter(channel => {
          return !!channel.is_member;
        })
        .map(channel => {
          return { id: channel.id, created: channel.created };
        });
    })
    .catch(err => {
      console.error(err);
      return [];
    });

  joinedChannels.forEach(async channel => {
    const messages = await user.channels
      .history({
        channel: channel.id,
        count: 1000,
        // チャンネル作成時間がそのチャンネルの一番古い時間のため oldest に設定.
        oldest: channel.created
      })
      .then(res => {
        // 新しい順で返ってくるので, 古い順にする.
        return (res.messages as any[]).reverse();
      })
      .catch(err => {
        console.error(err);
        return [];
      });

    // MAX_MESSAGE_NUMBER を超えていた場合, メッセージ数 - MAX_MESSAGE_NUMBER 分のメッセージを消す.
    if (messages.length > MAX_MESSAGE_NUMBER) {
      for (let i = 0; i < messages.length - MAX_MESSAGE_NUMBER; i++) {
        await user.chat
          .delete({ channel: channel.id, ts: messages[i].ts })
          .catch(err => {
            console.error(err);
          });
      }
    }
  });
})();