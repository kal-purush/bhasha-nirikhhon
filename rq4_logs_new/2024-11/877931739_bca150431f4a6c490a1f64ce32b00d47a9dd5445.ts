import { Entity, PrimaryGeneratedColumn, Column, CreateDateColumn, ManyToOne, JoinColumn } from 'typeorm';
import { randomUUID } from 'crypto';
import { Post } from '../../posts/domain/post.typeorm.entity';
import { User } from '../../../users/domain/user.typeorm.entity';

@Entity('Comments')
export class Comment {
    @PrimaryGeneratedColumn('uuid')
    id: string;

    @Column({ type: 'text', nullable: false })
    content: string;

    @CreateDateColumn({ type: 'timestamp', default: () => 'CURRENT_TIMESTAMP' })
    createdAt: Date;

    @Column({ type: 'uuid', nullable: false })
    postId: string;

    @ManyToOne(() => Post, { onDelete: 'CASCADE' })
    @JoinColumn({ name: 'postId' })
    post: Post;

    @Column({ type: 'uuid', nullable: false })
    userId: string;

    @ManyToOne(() => User, { onDelete: 'CASCADE' })
    @JoinColumn({ name: 'userId' })
    user: User;

    static createComment(postId: string, userId: string, content: string): Comment {
        const comment = new Comment();
    
        comment.id = randomUUID();
        comment.content = content;
        comment.createdAt = new Date();
        comment.postId = postId;
        comment.userId = userId;
    
        return comment;
    }
}