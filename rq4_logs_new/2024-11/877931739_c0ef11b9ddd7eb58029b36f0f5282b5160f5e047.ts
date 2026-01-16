import { Entity, Column, PrimaryGeneratedColumn, BaseEntity, PrimaryColumn, CreateDateColumn } from 'typeorm';
import { randomUUID } from 'crypto';

@Entity('Blogs')
export class Blog {
@PrimaryColumn('uuid')
id: string;

@Column({ type: 'varchar', nullable: false })
name: string;

@Column({ type: 'text', nullable: false })
description: string;

@Column({ type: 'varchar', nullable: false })
websiteUrl: string;

// @Column({ type: 'timestamp', nullable: false, default: () => 'CURRENT_TIMESTAMP' })
@CreateDateColumn()
createdAt: Date;

@Column({ type: 'boolean', nullable: false, default: false })
isMembership: boolean;

    static createBlog(name: string, description: string, websiteUrl: string): Blog {
        const blog = new Blog();
    
        blog.id = randomUUID();
        blog.name = name;
        blog.description = description;
        blog.websiteUrl = websiteUrl;
        blog.createdAt = new Date();
        blog.isMembership = false;
        
        return blog;
    }
}