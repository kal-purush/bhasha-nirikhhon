import * as restify from 'restify'
import * as mongoose from 'mongoose'
import * as Promise from 'bluebird'
import * as autoIncrement from 'mongoose-auto-increment'
import IBaseDocument from './IBaseDocument'
import { IPostDocument } from './post.model'
import { config } from '../../config/env'

enum StatType {
    views,
    likes
}

interface IStatValueRecord {
    pid: number;
    value: number
}

// Document interface
interface IStatDocument extends IBaseDocument {
    day: number;
    type: StatType;
    values: Array<IStatValueRecord>
}

// Model interface
interface IStatModel extends mongoose.Model<IStatDocument> {
    // statics
    findLastDayTopViewedPost: () => Promise<number>,
    findLastWeekTopViewedPost: () => Promise<number>,
    findLastMonthTopViewedPost: () => Promise<number>,
    updatePostViews: (pid: number) => Promise<number>
}

const StatSchema = new mongoose.Schema({
    id: {
        type: Number,
        unique: true,
        index: true
    },
    day: {
        type: Number,
        index: true
    },
    type: {
        type: String, // views/likes
        required: true,
        index: true
    },
    values: {
        type: Array,
        required: true
    }
})

// 挂载AutoIncrement插件
StatSchema.plugin(autoIncrement.plugin, {
    model: 'Stat',
    field: 'id',
    startAt: 0,
    incrementBy: 1
})

// Stat静态方法
StatSchema.statics = {
    /**
     * 获取昨日单日浏览数最高的文章ID
     * @returns {Promise<number>>}
     */
    findLastDayTopViewedPost: function (): Promise<number> {
        const now = new Date()
        const day = now.getFullYear() * 10000 + (now.getMonth() + 1) * 100 + now.getDate()
        return this.aggregate([
            { $match: { day: day, type: StatType.views } },
            { $unwind: '$values' },
            { $group: { _id: '$pid', total: { $sum: '$value' } } },
            { $sort: { 'total': -1 } },
            { $limit: 1 }
        ])
        .exec()
        .then((posts: any[]) => {
            if (posts && posts.length > 0) {
                return posts[0]._id
            }
            return 0
        })
    },

    /**
     * 获取过去一周内浏览数最高的文章ID
     * @returns {Promise<number>>}
     */
    findLastWeekTopViewedPost: function (): Promise<number> {
        const now = new Date()
        const day = now.getFullYear() * 10000 + (now.getMonth() + 1) * 100 + now.getDate()
        return this.aggregate([
            { $match: { day: { $gt: (day - 7) }, type: StatType.views } },
            { $unwind: '$values' },
            { $group: { _id: '$pid', total: { $sum: '$value' } } },
            { $sort: { 'total': -1 } },
            { $limit: 1 }
        ])
        .exec()
        .then((posts: any[]) => {
            if (posts && posts.length > 0) {
                return posts[0]._id
            }
            return 0
        })
    },

    /**
     * 获取过去一月内浏览数最高的文章ID
     * @returns {Promise<number>>}
     */
    findLastMonthTopViewedPost: function (): Promise<number> {
        const now = new Date()
        const day = now.getFullYear() * 10000 + (now.getMonth() + 1) * 100 + now.getDate()
        return this.aggregate([
            { $match: { day: { $gt: (day - 30) }, type: StatType.views } },
            { $unwind: '$values' },
            { $group: { _id: '$pid', total: { $sum: '$value' } } },
            { $sort: { 'total': -1 } },
            { $limit: 1 }
        ])
        .exec()
        .then((posts: any[]) => {
            if (posts && posts.length > 0) {
                return posts[0]._id
            }
            return 0
        })
    },

    /**
     * 更新文章的浏览数
     * @param {number} pid - 文章ID
     * @returns {Promise<number>}
     */
    updatePostViews: function (pid: number): Promise<number> {
        const now = new Date()
        const day = now.getFullYear() * 10000 + (now.getMonth() + 1) * 100 + now.getDate()
        return this.update({
            day: day,
            type: StatType.views,
            'values.pid': pid
        }, {
            $inc: { 'values.$.value': 1 }
        })
        .exec()
        .then(() => {
            Promise.resolve(pid)
        })
        .catch(() => {
            // 有今日的文档, 但是没有该文章的记录, 则附加该文章的浏览数记录
            this.update(
                { day: day, type: StatType.views },
                { $addToSet: { 'values': { 'pid': pid, 'value': 1 } } }
            )
            .exec()
            .then(() => {
                Promise.resolve(pid)
            })
            .catch(() => {
                // 没有今日的文档, 则创建它
                this.update(
                    { day: day, type: StatType.views},
                    { $setOnInsert: {
                        'values': { 'pid': pid, 'value': 1}
                    }},
                    { upsert: true }
                )
                .exec()
                .then(() => {
                    Promise.resolve(pid)
                })
                .catch((err: any) => {
                    Promise.reject(new Error(err))
                })
            })
        })
    }
}

const Stat: IStatModel = <IStatModel>mongoose.model<IStatDocument>('Stat', StatSchema)

export { Stat, IStatDocument, IStatModel }