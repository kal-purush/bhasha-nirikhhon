import {
  Entity,
  CreateDateColumn,
  PrimaryColumn,
  Column,
  JoinColumn,
  ManyToOne,
  OneToOne,
} from 'typeorm';
import { Packaging } from './packaging.entity';
import { MenuItem } from './menu-item.entity';
import { Media } from './media.entity';

@Entity('MenuItem_Packaging')
export class MenuItemPackaging {
  @PrimaryColumn({ type: 'int', nullable: false })
  public packaging_id: number;

  @PrimaryColumn({ type: 'int', nullable: false })
  public menu_item_id: number;

  @Column({ type: 'int', nullable: false })
  public image: number;

  @CreateDateColumn({
    type: 'datetime',
    nullable: false,
    unique: false,
    default: () => 'CURRENT_TIMESTAMP',
  })
  public created_at: Date;

  //RELATIONSHIP
  @ManyToOne(() => Packaging)
  @JoinColumn({
    name: 'packaging_id',
    referencedColumnName: 'packaging_id',
  })
  public packaging_obj: Packaging;

  @ManyToOne(() => MenuItem)
  @JoinColumn({
    name: 'menu_item_id',
    referencedColumnName: 'menu_item_id',
  })
  public menu_item_obj: MenuItem;

  @OneToOne(() => Media)
  @JoinColumn({
    name: 'image',
    referencedColumnName: 'media_id',
  })
  image_obj: Media;
}