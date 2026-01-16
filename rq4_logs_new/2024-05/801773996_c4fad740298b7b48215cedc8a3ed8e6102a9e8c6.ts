import { Controller, Get, Param } from '@nestjs/common';
import { ItemsService } from './items.service';

@Controller('items')
export class ItemsController {
  constructor(private readonly itemsService: ItemsService) {}

  @Get(':schoolId')
  public async getAllItems(@Param('schoolId') schoolId: string) {
    return this.itemsService.getAllItems(schoolId);
  }

  @Get(':schoolId/item/:itemId')
  public async getSingleItem(
    @Param('schoolId') schoolId: string,
    @Param('itemId') itemId: string,
  ) {
    return this.itemsService.getSingleItem(schoolId, itemId);
  }
}