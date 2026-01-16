import * as Twitter from 'twit';
import fetchMeta from '../../misc/fetch-meta';
import DriveFile, { IDriveFile } from '../../models/drive-file';
import Note, { INote } from '../../models/note';
import User, { ITwitterUser } from '../../models/user';
import uploadFromUrl from '../../services/drive/upload-from-url';
import post from '../../services/note/create';
import Resolver from '../activitypub/resolver';

export async function createImageFromTwitter(value: string, sensitive: boolean, actor: ITwitterUser): Promise<IDriveFile> {
	if (!value)
		return null;

	const instance = await fetchMeta();
	const cache = instance.cacheRemoteFiles;

	let file = await uploadFromUrl(value, actor, null, value, sensitive, false, !cache).catch(() => null);

	if (file.metadata.isRemote) {
		// URLが異なっている場合、同じ画像が以前に異なるURLで登録されていたということなので、
		// URLを更新する
		if (file.metadata.url !== value) {
			file = await DriveFile.findOneAndUpdate({ _id: file._id }, {
				$set: {
					'metadata.url': value,
					'metadata.uri': value
				}
			}, {
				returnNewDocument: true
			});
		}
	}

	return file;
}

export async function createUserFromTwitter(value: { user: { screen_name?: string, id_str?: string } } | string, resolver: Resolver): Promise<ITwitterUser> {
	await resolver.twitterPromise;

	if (typeof value === 'string') {

		if (!resolver.twitter)
			return null;

		const url = new URL(value);

		if (!`.${url.host}`.endsWith('.twitter.com'))
			return null;

		const segments = url.pathname.split('/');

		if (!segments[1].match(/^\w+$/))
			return null;

		return await createUserFromTwitter({
			user: {
				id_str: segments[1]
			}
		}, resolver);
	}

	const twitterUser: Twitter.Twitter.User = value.user.screen_name && value.user.id_str ? value.user : await resolver.twitter.get('users/show', {
		...(value.user.id_str ? { id: value.user.id_str } : { screen_name: value.user.screen_name }),
		tweet_mode: 'extended',
		...({ // For incorrect types
			include_ext_alt_text: true,
			include_profile_interstitial_type: true
		})
	}).then(
		x => x.data,
		_ => null);

	if (!twitterUser)
		return null;

	const createdAtCreatedAt = new Date(twitterUser.created_at);
	const createdAtSnowflake = new Date(Number(BigInt(twitterUser.id_str) >> 22n) + 1288834974657);
	const createdAt = createdAtCreatedAt.valueOf() / 1000 === ~~(createdAtSnowflake.valueOf() / 1000) ? createdAtSnowflake : createdAtCreatedAt;

	let user: ITwitterUser = await User.findOneAndUpdate({
		host: 'twitter.com',
		twitterId: twitterUser.id_str
	}, {
		$set: {
			createdAt,
			host: 'twitter.com',
			twitterId: twitterUser.id_str,
			uri: `https://twitter.com/intent/user?user_id=${twitterUser.id_str}`
		}
	}, {
		returnNewDocument: true,
		upsert: true
	});

	const sensitive = (twitterUser as any as Record<string, string>).profile_interstitial_type === 'sensitive_media';
	const avatarUrl = twitterUser.profile_image_url_https.replace('_normal', '');
	const bannerUrl = twitterUser.profile_banner_url;
	const avatar = avatarUrl && await createImageFromTwitter(avatarUrl, sensitive, user);
	const banner = bannerUrl && await createImageFromTwitter(bannerUrl, sensitive, user);
	const avatarColor = avatar && avatar.metadata.properties.avgColor ? avatar.metadata.properties.avgColor : null;
	const bannerColor = banner && avatar.metadata.properties.avgColor ? banner.metadata.properties.avgColor : null;

	user = await User.findOneAndUpdate({
		host: 'twitter.com',
		twitterId: twitterUser.id_str
	}, {
		$set: {
			updatedAt: new Date(),
			originalFollowersCount: twitterUser.followers_count,
			friendsCount: twitterUser.friends_count,
			name: twitterUser.name,
			notesCount: twitterUser.statuses_count,
			username: twitterUser.screen_name,
			usernameLower: twitterUser.screen_name.toLowerCase(),
			description: twitterUser.description,
			lang: twitterUser.lang,
			isLocked: twitterUser.protected,
			...(avatar ? {
				avatarId: avatar._id,
				avatarUrl,
				avatarColor
			} : {}),
			...(banner ? {
				bannerId: banner._id,
				bannerUrl,
				bannerColor
			} : {})
		}
	}, {
		returnNewDocument: true
	});

	return user;
}

export async function createNoteFromTwitter(value: any, resolver: Resolver, silent = false): Promise<INote> {
	if (typeof value !== 'string' || !resolver || typeof resolver !== 'object')
		return null;

	await resolver.twitterPromise;

	if (!resolver.twitter)
		return null;

	const url = new URL(value);

	if (!`.${url.host}`.endsWith('.twitter.com'))
		return null;

	const segments = url.pathname.split('/');

	if (segments[2] !== 'status' || !segments[3].match(/^\d+$/))
		return null;

	const tweet: Twitter.Twitter.Status = await resolver.twitter.get('statuses/show', {
		id: segments[3],
		tweet_mode: 'extended',
		...({ // For incorrect types
			include_ext_alt_text: true
		})
	}).then(
		x => x.data,
		_ => null);

	if (!tweet)
		return null;

	const user = await createUserFromTwitter(tweet, resolver);

	if (!user)
		return null;

	const uri = `https://twitter.com/i/web/status/${tweet.id_str}`;

	const note = await Note.findOne({ uri }) || await (async () => {
		const createdAtCreatedAt = new Date(tweet.created_at);
		const createdAtSnowflake = new Date(Number(BigInt(tweet.id_str) >> 22n) + 1288834974657);
		const createdAt = createdAtCreatedAt.valueOf() / 1000 === ~~(createdAtSnowflake.valueOf() / 1000) ? createdAtSnowflake : createdAtCreatedAt;

		const raw =
			tweet.full_text ||
			(tweet as any as Record<string, Record<string, string>>).extended_tweet && (tweet as any as Record<string, Record<string, string>>).extended_tweet.full_text ||
			tweet.text;

		type Replacer = Record<number, {
			to: string;
			next: number;
		}>;

		const replacers: Replacer = {
			...tweet.entities.hashtags.reduce<Replacer>((a, c) => (a[c.indices[0]] = {
				to: `#${c.text}`,
				next: c.indices[1]
			}, a), {}),
			...tweet.entities.urls.reduce<Replacer>((a, c) => (a[c.indices[0]] = {
				to: `<${c.expanded_url}>`,
				next: c.indices[1]
			}, a), {}),
			...tweet.entities.user_mentions.reduce<Replacer>((a, c) => (a[c.indices[0]] = {
				to: `@${c.screen_name}@twitter.com`,
				next: c.indices[1]
			}, a), {}),
			...(tweet.entities as any as { symbols: { text: string, indices: [number, number] }[] }).symbols.reduce<Replacer>((a, c) => (a[c.indices[0]] = {
				to: c.text,
				next: c.indices[1]
			}, a), {})
		};

		let text = '';

		for (let i = 0, p = i; i < raw.length; p = i) {
			text += replacers[i] ? (i = replacers[i].next, replacers[p].to) : raw[i++];
		}

		const reply = tweet.in_reply_to_status_id_str ? await createNoteFromTwitter(tweet.in_reply_to_status_id_str, resolver, silent) : null;

		const files = await Promise.all((tweet.entities.media || []).map(({ media_url_https }) => createImageFromTwitter(media_url_https, tweet.possibly_sensitive, user)));

		return await post(user, {
			createdAt,
			text,
			reply,
			files,
			viaTwitter: tweet.source,
			visibility: user.isLocked ? 'followers' : 'public',
			uri: `https://twitter.com/i/web/status/${tweet.id_str}`
		}, silent);
	})();

	return await Note.findOneAndUpdate({ _id: note._id }, {
		$set: {
			visibility: user.isLocked ? 'followers' : 'public',
			retweetCount: tweet.retweet_count,
			replyCount: (tweet as any as Record<string, number>).reply_count,
			reactionCounts: {
				twitter_favorite: tweet.favorite_count
			}
		}
	}, {
		returnNewDocument: true
	});
}