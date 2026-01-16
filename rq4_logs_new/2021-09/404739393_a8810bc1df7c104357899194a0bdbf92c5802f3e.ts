import { User } from 'src/user/entity/user.entity';
import { Column, Entity, JoinColumn, ManyToOne, PrimaryColumn } from 'typeorm';
import { Match } from './match.entity';

@Entity('matchPlayer')
export class MatchPlayer {
  @ManyToOne((type) => Match, (match) => match.players)
  @JoinColumn({ name: 'matchId' })
  match: Match;
  @PrimaryColumn()
  matchId: number;

  @ManyToOne(() => User)
  @JoinColumn({ name: 'userId' })
  user: User;
  @PrimaryColumn()
  userId: number;

  @Column()
  score: number;
  @Column()
  isLeft: boolean;
  @Column()
  ladderPoint: number;
  @Column()
  ladderIncrease: number;
}