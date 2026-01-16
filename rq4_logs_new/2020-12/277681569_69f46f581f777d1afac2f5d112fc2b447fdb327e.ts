import {
  Column, Entity, PrimaryGeneratedColumn,
} from 'typeorm';

// 방송 진행 여부에 따라 지속 업데이트 되는 테이블이며 가장 최신의 방송정보들을 가지고 있다.(트루포인트에서 아프리카를 구독한 회원만큼의 row 존재, 지속 업데이트 됨)
// - videoId(PK) : 아프리카 방송을 Luke의 자체적인 고윳값을 지정한 값
// - creatorId : 크리에이터ID
// - creatorName : 크리에이터 활동명
// - is_live : 방송 시작 및 종료 여부
// - startDate : 방송 시작시간
// - endDate : 방송 종료시간
@Entity('AfreecaActiveStreams')
export class AfreecaActiveStreamsEntity {
  @PrimaryGeneratedColumn()
  id: string;

  @Column()
  creatorId: string;

  @Column()
  creatorName: string;

  @Column({ default: false })
  is_live?: boolean;

  @Column({ default: false })
  is_private?: boolean;

  @Column({ type: 'timestamp' })
  startDate: Date;

  @Column({ type: 'timestamp' })
  endDate: Date;
}