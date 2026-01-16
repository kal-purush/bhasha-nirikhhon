import $ from 'cafy';
import ID, { transform } from '../../../../misc/cafy-id';
import define from '../../define';
import User, { pack } from '../../../../models/user';

export const meta = {
	desc: {
		'ja-JP': '指定したユーザーの属性を更新します。',
		'en-US': 'Update user attributes.'
	},

	tags: ['admin'],

	requireCredential: true,
	requireModerator: true,

	params: {
		userId: {
			validator: $.type(ID),
			transform: transform,
			desc: {
				'ja-JP': '対象のユーザーID',
				'en-US': 'The user ID which you want to update'
			}
		},

		isBot: {
			validator: $.optional.bool,
			desc: {
				'ja-JP': 'Botか否か'
			}
		},

		isCat: {
			validator: $.optional.bool,
			desc: {
				'ja-JP': '猫か否か'
			}
		},

		isKaho: {
			validator: $.optional.bool,
			desc: {
				'ja-JP': 'ｺﾐﾔｶﾎか否か'
			}
		},
	}
};

export default define(meta, async (ps, me) => {
	const user = await User.findOne({
		_id: ps.userId
	});

	if (!user) {
		throw new Error('user not found');
	}

	if (user.isAdmin) {
		throw new Error('cannot update admin');
	}

	return await pack(await User.findOneAndUpdate({
		_id: user._id
	}, {
		$set: {
			...(typeof ps.isBot === 'boolean' ? { isBot: ps.isBot } : {}),
			...(typeof ps.isCat === 'boolean' ? { isCat: ps.isCat } : {}),
			...(typeof ps.isKaho === 'boolean' ? { isKaho: ps.isKaho } : {}),
		}
	}, { returnNewDocument: true }), me);
});