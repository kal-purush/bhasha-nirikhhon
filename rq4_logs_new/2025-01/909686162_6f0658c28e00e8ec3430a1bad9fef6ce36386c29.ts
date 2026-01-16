import { Body, Controller, Delete, Get, Param, ParseIntPipe, Patch, Post, Req, UseGuards } from '@nestjs/common';
import { BucketListItemsService } from './bucket-list-items.service';
import { AccessTokenGuard } from 'src/common/guards/access-token.guard';
import { CreateBucketListDto } from 'src/bucket-lists/dto/create-bucket-list.dto';
import { CreateBucketListItemDto } from './dto/create-bucket-list-item.dto';
import { Request } from 'express';
import { UpdateBucketListItemDto } from './dto/update-bucket-list-item.dto';

@Controller('bucket-lists')
export class BucketListItemsController {
  constructor(
    private readonly bucketListItemsService : BucketListItemsService
  ) {

  }

  @Post('/:bucketListId/items')
  @UseGuards(AccessTokenGuard)
  async createBucketListItem(
    @Req() req : Request,
    @Param('bucketListId' , ParseIntPipe) bucketListId : number,
    @Body() createBucketListItemDto : CreateBucketListItemDto
  ) {
    const userId = req.user['id']
    return this.bucketListItemsService.create(
        userId,
        bucketListId,
        createBucketListItemDto
    );
  }

  @Get('/:bucketListId/items')
  @UseGuards(AccessTokenGuard) 
  async findAllBucketListItems(
    @Req() req : Request,
    @Param('bucketListId' , ParseIntPipe) bucketListId : number,
  ) {
    const userId = req.user['id']
    return this.bucketListItemsService.findAll(userId, bucketListId);
  }

  @Patch('/:bucketListId/items/:itemId')
  @UseGuards(AccessTokenGuard)
  async updateBucketListItem(
    @Req() req : Request,
    @Param('bucketListId' , ParseIntPipe) bucketListId : number,
    @Param('itemId' , ParseIntPipe) itemId : number,
    @Body() updateBucketListItemDto : UpdateBucketListItemDto
  ) {
    const userId = req.user['id']
    return this.bucketListItemsService.update(
      userId,
      bucketListId,
      itemId,
      updateBucketListItemDto
    )
  }

  @Delete('/:bucketListId/items/:itemId')
  @UseGuards(AccessTokenGuard)
  async deleteBucketListItem(
    @Req() req : Request,
    @Param('bucketListId' , ParseIntPipe) bucketListId : number,
    @Param('itemId' , ParseIntPipe) itemId : number
  ) {
    const userId = req.user['id']
    return this.bucketListItemsService.remove(
      userId,
      bucketListId,
      itemId
    )
  }
}