import { Entity, Column, ManyToOne, JoinColumn } from 'typeorm';
import { BaseEntity } from 'src/common/entities/base.entity';
import { DocumentType } from 'src/modules/documentsTypes/entity/document-type.entity';
import { User } from 'src/modules/users/entity/user.entity';

@Entity()
export class Document extends BaseEntity {
  @Column({ nullable: false })
  fileUrl: string;

  @Column()
  isPaid: boolean;

  @Column()
  expiredAt: string;

  @ManyToOne(() => User, (user) => user.documents, {
    onDelete: 'CASCADE',
  })
  @JoinColumn({ name: 'userId' })
  user: User;

  @ManyToOne(() => DocumentType, (type) => type.documents)
  @JoinColumn({ name: 'documentTypeId' })
  documentType: DocumentType;
}