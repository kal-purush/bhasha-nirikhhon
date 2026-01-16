import { Types } from 'mongoose';
import User from '../../models/user';
import * as promise from 'bluebird';
import * as config from 'config';

const userModel = promise.promisifyAll(User);

const getAuth = async (id: string) => {
    try {
        const d = await userModel
            .findById(id)
            .select({ _id: 1, username: 1, email: 1, avatar_url: 1 });
        if (!d) {
            throw new Error('Not found.');
        }
        if (!d.avatar_url) {
            d.avatar_url = config.get('default-user-avatar');
        }
        return d;
    } catch (error) {
        console.error(error);
    }
};
const getFriendsList = async (id: string) => {
    try {
        const d = await userModel.aggregate([
            {
                $match: { _id: Types.ObjectId(id) }
            },
            {
                $project: {
                    _id: 1,
                    user_friends: {
                        $filter: {
                            input: '$user_friends',
                            as: 'user_friend',
                            cond: {
                                $eq: ['$$user_friend.status', 1]
                            }
                        }
                    }
                }
            },
            {
                $lookup: {
                    from: 'users',
                    localField: 'user_friends._id',
                    foreignField: '_id',
                    as: 'user_friends'
                }
            },
            {
                $project: {
                    _id: 1,
                    username: 1,
                    email: 1,
                    avatar_url: {
                        $ifNull: [
                            '$avatar_url',
                            config.get('default-user-avatar')
                        ]
                    },
                    'user_friends._id': 1,
                    'user_friends.username': 1,
                    'user_friends.email': 1,
                    'user_friends.avatar_url': {
                        $ifNull: [
                            '$avatar_url',
                            config.get('default-user-avatar')
                        ]
                    }
                }
            }
        ]);
        return d[0].user_friends;
    } catch (error) {
        console.error(error);
    }
};

export { getAuth, getFriendsList };