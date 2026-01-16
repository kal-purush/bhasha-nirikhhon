import fs from 'fs';

const handler = async (m, { conn, text, usedPrefix, command }) => {

await m.reply('Bot working..')

}

handler.command = ['alive']
handler.admin = true;
export default handler;