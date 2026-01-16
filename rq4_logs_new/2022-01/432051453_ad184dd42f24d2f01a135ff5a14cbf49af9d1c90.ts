import DataLoader from 'dataloader';

import { Injectable, Scope } from '@nestjs/common';

import { CategoriesService } from './categories.service';

@Injectable({ scope: Scope.REQUEST })
export class CategoryLoaders {
  constructor(private categoryService: CategoriesService) {}

  public readonly batchCategories = new DataLoader(
    async (masterIds: number[]) => {
      const masters = await this.categoryService.getByIds(masterIds);
      const mastersMap = new Map(masters.map((item) => [item.id, item]));
      return masterIds.map((masterId) => mastersMap.get(masterId));
    },
  );
}