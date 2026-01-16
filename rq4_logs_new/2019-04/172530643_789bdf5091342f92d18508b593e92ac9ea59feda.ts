import { scheduleJob } from 'node-schedule';
import fetchMeta from '../misc/fetch-meta';
import User, { IUser } from '../models/user';
import create from '../services/note/create';

const text = `みなさん ! おはようございまーっす♪
今日も一日、元気いっぱいがんばろー !
#双葉杏bot`;

const sevenOClock = (date: Date) => (date.setHours(7, 0, 0, 0), date);

export default function() {
	scheduleJob('futaba-anzu-bot', { hour: 7 }, date => fetchMeta()
		.then(meta => User.findOne({
			usernameLower: meta.futabaAnzuBotAccount.toLowerCase(),
			host: null
		}))
		.then(user => user || Promise.reject<IUser>('Anzu bot not found.'))
		.then(user => create(user, {
			createdAt: sevenOClock(date),
			text
		})));
}