import { LikesAggregate } from '../likes.aggregate';
import { Types } from 'mongoose';
import { ICommandHandler, EventPublisher, CommandHandler } from '@nestjs/cqrs';
import { CreateLikePayloadDto } from '../../../models/likes/dto/create-like.dto';
import { Inject, BadRequestException } from '@nestjs/common';
import { LikeCreated } from '../events/like-created.event';
import { UserLean } from '../../../models/users/user.interface';
import { CreateLikePayload, likeType } from '../../../models/likes/likes.interface';
import { UsersService } from '../../../models/users/users.service';

export class CreateLike {
  constructor(public data: CreateLikePayloadDto, public user: UserLean) {}
}
@CommandHandler(CreateLike)
export class CreateLikeHandler implements ICommandHandler<CreateLike> {
  @Inject() private readonly _publisher: EventPublisher;
  @Inject() private readonly _usersService: UsersService;

  async execute({ data, user }: CreateLike): Promise<Boolean> {
    const userFromDB = await this._usersService.getById(user._id);
    console.log(data);
    if(!Object.values(likeType).includes(data.type)) {
      throw new BadRequestException(`Type has to be either like or share`);
    }

    if (!userFromDB) {
      throw new BadRequestException(`This user doesn't exist`);
    }

    const likeData: CreateLikePayload = {
      ...data,
      user: userFromDB._id
    };
    const aggregate = new LikesAggregate();

    const id = new Types.ObjectId().toHexString();

    aggregate.apply(new LikeCreated(id, likeData));
    
    const advert = this._publisher.mergeObjectContext(aggregate);
    advert.commit();

    return true;
  }
}