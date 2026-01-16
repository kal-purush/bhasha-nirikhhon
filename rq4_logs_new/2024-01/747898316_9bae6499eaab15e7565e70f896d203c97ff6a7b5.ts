import { Crud, CrudController } from '@dataui/crud'
import { Controller, UseGuards, UseInterceptors } from '@nestjs/common'
import { ApiTags } from '@nestjs/swagger'
import { JwtAuthGuard } from 'src/common/guards/auth.guard'
import { ABACValidator } from 'src/common/interceptors/abac.interceptor'
import { Comments } from 'src/db/entities/comments.entity'
import { CommentsService } from './comments.service'
import { commentsCrud } from './comments.crud'

@Controller('cards/:cardsId/comments')
@ApiTags('comments')
@Crud(commentsCrud)
@UseGuards(JwtAuthGuard)
@UseInterceptors(ABACValidator(Comments))
export class CommentsController implements CrudController<Comments> {
  constructor(public service: CommentsService) {}
  get base(): CrudController<Comments> {
    return this
  }
}