
import { Lancamento } from 'src/lancamento/entities/lancamento.entity';
import { EntityHelper } from 'src/utils/entity-helper';
import { asStringOrNumber } from 'src/utils/pipe-utils';
import { AfterLoad, Column, CreateDateColumn, DeepPartial, Entity, OneToOne, PrimaryGeneratedColumn, UpdateDateColumn } from 'typeorm';

@Entity()
export class OrdemPagamentoEntity extends EntityHelper {
  constructor(dto?: DeepPartial<OrdemPagamentoEntity>) {
    super();
    if (dto) {
      Object.assign(this, dto);      
    }
  }

  @PrimaryGeneratedColumn({ primaryKeyConstraintName: 'PK_Ordem_Pagamento_id' })
  id: number; 

  @CreateDateColumn()
  dataProcessamento: Date;

  @CreateDateColumn()
  dataCaptura: Date;

  @Column({ type: String, unique: false, nullable: true, length: 200 })
  nomeConsorcio: string | null;

  @Column({ type: String, unique: false, nullable: true, length: 200 })
  nomeOperadora: string | null;
   
  @Column({ type: String, unique: false, nullable: false })
  userId: number;

  @Column({
    type: 'decimal',
    unique: false,
    nullable: true,
    precision: 13,
    scale: 5,
  })
  valor: number;

  @Column({ type: String, unique: false, nullable: true })
  idOrdemPagamento: string | null;

  /** CPF. */
  @Column({ type: String, unique: false, nullable: true })
  idOperadora: string | null;

  /** CNPJ */
  @Column({ type: String, unique: false, nullable: true })
  idConsorcio: string | null;

  @Column({ type: Date, unique: false, nullable: false })
  dataOrdem: Date;
  
  @CreateDateColumn()
  createdAt: Date;

  @UpdateDateColumn()
  updatedAt: Date;

  /** Não é uma coluna, usado apenas para consulta no ORM. */
  @OneToOne(() => Lancamento, (l) => l.itemTransacao)
  lancamento: Lancamento | null;  

  @AfterLoad()
  setReadValues() {
    this.valor = asStringOrNumber(this.valor);
  }
}