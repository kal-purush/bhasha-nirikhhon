import {
    AllowNull,
    Column,
    DataType,
    ForeignKey,
    HasMany,
    Length,
    Model,
    PrimaryKey,
    Table,
} from 'sequelize-typescript'
import type { Optional } from 'sequelize'
import { Topic } from './topic.model'
import { ERRORS } from '../error/constants'

export interface CommentAttributes {
    id: number
    topicId: number
    content: string
    userId: number
    parentId: number
}
export type CommentCreationAttributes = Optional<
    CommentAttributes,
    'id' | 'parentId'
>

@Table({
    tableName: 'comments',
})
export class Comment extends Model<
    CommentAttributes,
    CommentCreationAttributes
> {
    @PrimaryKey
    @Column
    declare id: number

    @ForeignKey(() => Topic)
    @AllowNull(false)
    @Column(DataType.INTEGER)
    declare topicId: number

    @ForeignKey(() => Comment)
    @Column(DataType.INTEGER)
    declare parentId: number

    @Length({ max: 1000, msg: ERRORS.maxLength })
    @AllowNull(false)
    @Column(DataType.TEXT)
    declare content: string

    @AllowNull(false)
    @Column(DataType.INTEGER)
    declare userId: number

    @HasMany(() => Comment, 'parentId')
    declare replies: Comment[]
}