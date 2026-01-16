import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Post } from 'src/post/post.entity';
import { Thread } from 'src/thread/thread.entity';
import { Repository } from 'typeorm';


@Injectable()
export class IdService {

    constructor(
        @InjectRepository(Thread)
        private threadRepository: Repository<Thread>,
        @InjectRepository(Post)
        private postRepository: Repository<Post>) {
        this.initCurrentId()
    }

    private static currentId: number = 0;

    private async initCurrentId() {
        const maxThreadId = (await this.threadRepository.createQueryBuilder().select("MAX(Thread.threadId)", "max").getRawOne()).max
        const maxPostId = (await this.postRepository.createQueryBuilder().select("MAX(Post.postId)", "max").getRawOne()).max
        let maxId;

        if (maxThreadId && maxPostId) {
            maxId = Math.max(maxThreadId, maxPostId)
        }
        else if (!maxThreadId && maxPostId) {
            maxId = maxPostId
        }
        else if (maxThreadId && !maxPostId) {
            maxId = maxThreadId
        }
        else {
            maxId = 0
        }
        
        IdService.currentId = maxId        
    }

    public getCurrentId() {
        IdService.currentId++;
        return IdService.currentId;
    }
}