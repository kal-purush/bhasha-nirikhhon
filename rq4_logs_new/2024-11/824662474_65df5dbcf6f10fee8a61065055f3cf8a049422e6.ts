import { Injectable, Inject } from "@nestjs/common";
import { ClientProxy } from '@nestjs/microservices';
import { lastValueFrom } from "rxjs";
import { ItemDTO } from "../../pkg/dtos/item.dto";
import { ItemCategory } from "../../core/entities/item-categories.entity";

@Injectable()
export class ExternalItemService {
  constructor(
    @Inject('ITEMS_MICROSERVICE') private readonly itemMicroserviceClient: ClientProxy,
  ) {}

  async createItem(item: ItemDTO) {
    const res = this.itemMicroserviceClient.send('create_item', { ...item });
    const val = await lastValueFrom(res);
    return val;
  }

  async getItems() {
    const req = this.itemMicroserviceClient.send('get_all_items', {});
    const val = await lastValueFrom(req);
    return val;
  }

  async getItemsPerCategory(category: ItemCategory) {
    const req = this.itemMicroserviceClient.send('get_items_per_category', category);
    const val = await lastValueFrom(req);
    return val;
  }

  async getItemById(id: number) {
    const req = this.itemMicroserviceClient.send('get_item_by_id', id);
    const val = await lastValueFrom(req);
    return val;
  }
}