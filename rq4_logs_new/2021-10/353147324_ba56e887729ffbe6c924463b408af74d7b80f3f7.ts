import {
  Column,
  Entity,
  JoinColumn,
  ManyToOne,
  ObjectType,
} from 'typeorm';
import Base from './base';
import EntidadeUsuario from './usuario';

@Entity('solicitacao')
export default class EntidadeSolicitacao extends Base {
  @Column()
  public nome?: string;
  
  @Column({ type: 'int4' })
  public tipo?: number;

  @Column({ type: 'int4' })
  public situacao?: number;
  
  @Column({ type: 'date' })
  public dataInclusao?: string;

  @Column({ type: 'uuid' })
  public idSolicitante?: string;

  @ManyToOne(
    (): ObjectType<EntidadeUsuario> => EntidadeUsuario,
    (usuario: EntidadeUsuario): EntidadeSolicitacao[] => usuario.solicitacoes,
  )
  @JoinColumn({ name: 'idSolicitante', referencedColumnName: 'id' })
  public usuario?: EntidadeUsuario;
}