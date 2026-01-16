import { IsNumber } from 'class-validator';
import { Content } from 'src/content/entities/content.entity';
import { Display } from 'src/display/entities/display.entity';
import { Entity, ManyToMany, ManyToOne, OneToOne, PrimaryGeneratedColumn } from 'typeorm';
import { Playlist } from './playlist.entity';

@Entity()
export class PlaylistUnit {
  @PrimaryGeneratedColumn('uuid') id: string;

	@ManyToOne(()=> Playlist, playlist => playlist.units)
	playlist: Playlist;

  @OneToOne(() => Display, (display) => display.playlist)
  display: Display;

	@IsNumber()
	order: number;

  @ManyToMany(() => Content, (content) => content.playlists)
  content: Map<number,Content>;
}