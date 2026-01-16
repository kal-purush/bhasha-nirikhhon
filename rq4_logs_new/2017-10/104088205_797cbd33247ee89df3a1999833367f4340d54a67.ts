import { Table, Column, Model, DataType, ForeignKey } from 'sequelize-typescript';
import { SymbolInfo } from '../model';

@Table({
  timestamps: true,
  paranoid: true,
  underscored: true,
  comment: 'DDE实时数据'
})
export default class Dde extends Model<Dde> {

  @Column({
    type: DataType.STRING(10),
    comment: '服务名'
  })
  service: string;

  @Column({
    type: DataType.STRING(20),
    comment: '主题名'
  })
  topic: string;

  @Column({
    type: DataType.STRING(50),
    comment: '数据项目名'
  })
  item: string;

  @Column({
    type: DataType.STRING(50),
    comment: '数据内容'
  })
  text: string;

  @Column
  get created_at(): string {
    return this.getDataValue('created_at');
  }

  @Column
  get updated_at(): string {
    return this.getDataValue('updated_at');
  }

  @Column
  get deleted_at(): string {
    return this.getDataValue('deleted_at');
  }
}