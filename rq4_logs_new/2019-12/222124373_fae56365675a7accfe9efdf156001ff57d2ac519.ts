import { Column, Entity, OneToMany, PrimaryGeneratedColumn } from 'typeorm';
import Ingredient from './ingredient';

@Entity()
export default class Recipe {

  @PrimaryGeneratedColumn()
  public id: number;

  @Column()
  public title: string;

  @Column({ nullable: true })
  public description: string;

  @Column({ nullable: true })
  public image: string;

  @OneToMany(() => Ingredient, (ingredient) => ingredient.recipe, {
    eager: true,
  })
  public ingredients: Ingredient[];

  @Column()
  public duration: string;

  @Column({ default: 0 })
  public rating: number;

}