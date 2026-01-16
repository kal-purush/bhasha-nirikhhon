import { Column, Entity, ManyToOne, OneToMany, PrimaryGeneratedColumn, Unique } from 'typeorm';
import { UsuarioEntity } from '../usuario/usuario.entity';
import { DetCarritoEntity } from '../det_carrito/det-carrito.entity';

@Entity("cab_carrito")
export class CabCarritoEntity {

  @PrimaryGeneratedColumn({
    type:'int',
    unsigned:true,
    name:'id_cab_carrito',
    comment:'identificador de la tabla cab-carrito'
  })
  id:number;

  @Column({
    type:"varchar",
    name:"estado_cab_carrito",
    nullable:false,
    comment:"estado de la tabla cab_carrito"
  })
  estado:string;

  @Column({
    type:"date",
    name:"fecha_cab_carrito",
    nullable:true,
    comment:"fecha de la tabla cab_carrito"
  })
  fecha:string;

  @Column({
    type:"double",
    name:"total_cab_carrito",
    nullable:false,
    comment:"total de la tabla cab_carrito"
  })
  total:number;

  @Column({
    type:"varchar",
    name:"direccion_cab_carrito",
    nullable:false,
    comment:"direccion de la tabla cab_carrito"
  })
  direccion:string;

  @ManyToOne(
    type => UsuarioEntity,
    usuario=> usuario.cabCarrito
  )
  usuario:UsuarioEntity;

  @OneToMany(
    type => DetCarritoEntity,
    detalle=>detalle.cab
  )
  detalle:DetCarritoEntity[];
}