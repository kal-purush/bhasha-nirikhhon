import { Entity, Column, PrimaryGeneratedColumn } from "typeorm";

@Entity()
export class Article {
  @PrimaryGeneratedColumn()
  id!: number;

  @Column()
  top_priority_number!: string;

  @Column()
  top_display_flag!: boolean;

  @Column()
  news_id!: string;

  @Column()
  news_prearranged_time!: string;

  @Column("text")
  title!: string;

  @Column("text")
  title_with_ruby!: string;

  @Column("text")
  outline_with_ruby!: string;

  @Column()
  news_file_ver!: boolean;

  @Column()
  news_publication_status!: boolean;

  @Column()
  has_news_web_image!: boolean;

  @Column()
  has_news_web_movie!: boolean;

  @Column()
  has_news_easy_image!: boolean;

  @Column()
  has_news_easy_movie!: boolean;

  @Column()
  has_news_easy_voice!: boolean;

  @Column("text")
  news_web_image_uri!: string;

  @Column("text")
  news_web_movie_uri!: string;

  @Column("text")
  news_easy_image_uri!: string;

  @Column("text")
  news_easy_movie_uri!: string;

  @Column("text")
  news_easy_voice_uri!: string;
}