import {Entity, Column, BaseEntity, CreateDateColumn, PrimaryColumn, ManyToOne, JoinColumn, PrimaryGeneratedColumn} from "typeorm";
import PrayArticle from "./PrayArticle";
@Entity()
export default class PrayAmen extends BaseEntity {
  @PrimaryGeneratedColumn({type: "bigint"})
  amenid: number;
  @Column({type: "bigint"})
  articleid: number;

  @Column()
  userid: string;

  @ManyToOne(() => PrayArticle, article => article.amen)
  @JoinColumn({name: "articleid"})
  article: PrayArticle;

  @CreateDateColumn()
  createDate: Date;
}