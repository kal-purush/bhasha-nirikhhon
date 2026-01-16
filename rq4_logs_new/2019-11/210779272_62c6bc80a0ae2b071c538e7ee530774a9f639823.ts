import {
  VuexModule,
  Module,
  Action,
  Mutation,
  getModule,
} from 'vuex-module-decorators';
import store from '@/store';

import { IInternshipTypes } from '@/api/types';
import { getCategories } from '@/api/categories';

@Module({ dynamic: true, store, name: 'categories' })
class Categories extends VuexModule {
  private all: IInternshipTypes[] = [];

  public get categories() {
    return this.all || [];
  }

  @Mutation
  private SET_CATEGORIES(data: IInternshipTypes[]) {
    this.all = data;
  }

  @Action
  public getCategory(id: number): IInternshipTypes | undefined {
    const found = this.all.findIndex(c => c.id === id);
    if (found !== -1) return this.all[found];
    return undefined;
  }

  @Action
  public setup(): Promise<void> {
    return new Promise((resolve, reject) => {
      getCategories()
        .then((res: any) => {
          this.SET_CATEGORIES(res || []);
          resolve();
        })
        .catch((e: any) => reject(e));
    });
  }
}

export const CategoriesModule = getModule(Categories);