import { Model, STRING, INTEGER, Sequelize, TEXT } from 'sequelize'

class ArtPage extends Model {
  public id!: number
  public title!: string
  public desc!: string
  public cover!: string
  public tags!: string
  public pid!: number
  public content!: string
  public state!: number
  public ctime!: string
  public mtime!: string
  public level!: number
  public extra!: string
}

export default ArtPage

export const modelItem = {
  id: {
    type: INTEGER().UNSIGNED,
    autoIncrement: true,
    comment: '自增id',
    primaryKey: true
  },
  pid: {
    type: INTEGER().UNSIGNED,
    allowNull: true,
    defaultValue: 0,
    comment: '父级 id'
  },
  state: {
    type: INTEGER,
    allowNull: false,
    defaultValue: 1,
    comment: '-1 删除，1 已发布，2 草稿'
  },
  cover: {
    type: STRING(255),
    allowNull: true,
    defaultValue: '',
    comment: '缺省'
  },
  extra: {
    type: STRING(255),
    allowNull: true,
    defaultValue: '',
    comment: '额外字段，不暴露给前端'
  },
  tags: {
    type: STRING(255),
    allowNull: true,
    comment: '标签'
  },
  title: {
    type: STRING(255),
    allowNull: false,
    defaultValue: '无题',
    comment: '文章标题'
  },
  desc: {
    type: STRING(255),
    allowNull: true,
    comment: '描述'
  },
  content: {
    type: TEXT,
    allowNull: false,
    defaultValue: '',
    comment: '内容，格式：html'
  },
  level: {
    type: INTEGER,
    allowNull: true,
    defaultValue: 0,
    comment: 'tree 层级'
  }
}

export function init(sequelize: Sequelize) {
  return ArtPage.init(modelItem, {
    timestamps: false,
    tableName: 'art_page',
    sequelize,
    createdAt: 'ctime',
    updatedAt: 'mtime',
    deletedAt: false,
    paranoid: false
  })
}