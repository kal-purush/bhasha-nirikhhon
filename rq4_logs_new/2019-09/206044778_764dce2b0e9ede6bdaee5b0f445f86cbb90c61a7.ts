import {
  EventSubscriber,
  EntitySubscriberInterface,
  InsertEvent,
  Connection,
} from 'typeorm';
import { Comment } from '../../db/models/comment.model';
import { Inject, Injectable } from '@nestjs/common';
import { MailService } from './mail.service';
import { IdeasDBServiceInterface } from '../../ideas/interfaces/ideas.db.service.interface';
import { UsersDBService } from '../../db/services/users.db.service';
import { MailTemplateService } from './mail-template.service';
import { DEFAULT_DOMAIN } from '../../../constants';

@Injectable()
@EventSubscriber()
export class CommentsSubscriber implements EntitySubscriberInterface<Comment> {
  constructor(
    @Inject('MONGO_DB_CONNECTION') private readonly connection: Connection,
    @Inject('IdeasDBService')
    private readonly ideasDBService: IdeasDBServiceInterface,
    @Inject('UsersDBService') private readonly usersDBService: UsersDBService,
    private readonly mailer: MailService,
    @Inject(MailTemplateService)
    protected readonly templateService: MailTemplateService,
  ) {
    this.connection.subscribers.push(this);
  }

  listenTo() {
    return Comment;
  }

  async afterInsert(event: InsertEvent<Comment>) {
    const comment = event.entity;
    const idea = await this.ideasDBService.findById(comment.ideaId);
    const user = await this.usersDBService.findById(idea.owner);

    return await this.mailer.send(
      user.email,
      'New comment for idea: ' + idea.title,
      await this.templateService.getMailBody('new-comment', {
        idea,
        comment,
        ideaLink: `${DEFAULT_DOMAIN}/idea/${idea.id}`,
      }),
    );
  }
}