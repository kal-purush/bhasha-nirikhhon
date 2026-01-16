import { Repository } from 'typeorm';
import { Reply } from '../entities/reply';
import { Dependencies } from '../types';
import { QueryDeepPartialEntity } from 'typeorm/query-builder/QueryPartialEntity';

export class ReplyService {
  repository: Repository<Reply>;
  constructor(options: Dependencies) {
    this.repository = options.replyRepository;
  }

  find() {
    return this.repository.find();
  }

  findOne(id: number) {
    return this.repository.findOne(id);
  }

  save() {}

  delete(id: number) {
    return this.repository.delete(id);
  }

  update(id: number, obj: QueryDeepPartialEntity<Reply>) {
    return this.repository.update(id, obj);
  }
}