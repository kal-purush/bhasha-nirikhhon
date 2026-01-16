import { Column, Entity, JoinColumn, ManyToOne, PrimaryColumn } from "typeorm";
import { User } from "./User";
import { Book } from "./Book";
import { BaseEntity } from "./BaseEntity";

@Entity({ name: "votes" })
export class Vote extends BaseEntity{
  @ManyToOne(() => User, (user) => user.votes)
  @JoinColumn({ name: "user_id" })
  user: User;

  @ManyToOne(() => Book, (book) => book.votes, {
    cascade: true,
    onDelete: "CASCADE",
    onUpdate: "CASCADE",
  })
  @JoinColumn({ name: "book_id" })
  book: Book;
}