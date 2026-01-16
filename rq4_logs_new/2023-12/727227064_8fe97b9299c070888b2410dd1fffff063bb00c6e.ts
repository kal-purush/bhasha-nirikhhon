import { Role } from "src/enums/role.enum";
import { Column, CreateDateColumn, Entity, PrimaryGeneratedColumn, UpdateDateColumn } from "typeorm";

@Entity({
  name: 'users'
})
export class UserEntity {

  @PrimaryGeneratedColumn()
  id: number;

  @Column({
    length: 63,
    nullable: true
  })
  name: string;

  @Column({
    length: 127,
    unique: true,
    nullable: true
  })
  email: string;

  @Column({
    length: 127,
    nullable: true
  })
  password: string;

  @Column({
    type: "date",
    nullable: true
  })
  birthday: Date;

  @CreateDateColumn()
  createdat: string;

  @UpdateDateColumn()
  updatedat: string;

  @Column({
    default: Role.User
  })
  role: number;
}