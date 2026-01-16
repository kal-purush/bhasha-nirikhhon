import { Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { CommentDto } from '../dto/comment.dto';
import { CommentDocument } from '../interfaces/comment.document';

@Injectable()
export class CommentRepository {
  constructor(
    @InjectModel('comment')
    private readonly commentModel: Model<CommentDocument>,
  ) {}
  getMany(query: object): Promise<CommentDocument[]> {
    return this.commentModel.find(query).populate('user').exec();
  }

  getOne(query: object): Promise<CommentDocument> {
    return this.commentModel.findOne(query).populate('user').exec();
  }

  create(commentData: CommentDto): Promise<CommentDocument> {
    return this.commentModel.create(commentData);
  }

  update(query: object, commentData: object): Promise<CommentDocument> {
    return this.commentModel
      .findOneAndUpdate(query, commentData, { new: true })
      .exec();
  }

  delete(query: object): Promise<CommentDocument> {
    return this.commentModel
      .findOneAndUpdate(query, { status: false }, { new: true })
      .exec();
  }
}