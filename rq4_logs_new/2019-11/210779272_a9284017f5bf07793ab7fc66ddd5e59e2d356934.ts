import {
  VuexModule,
  Module,
  Action,
  Mutation,
  getModule,
} from 'vuex-module-decorators';
import store from '@/store';

import { ICampaigns } from '@/api/types';
import { getCampaigns } from '@/api/campaigns';

@Module({ dynamic: true, store, name: 'campaigns' })
class Campaigns extends VuexModule {
  private all: ICampaigns[] = [];

  public get campaigns() {
    return this.all || [];
  }

  @Mutation
  private SET_CAMPAIGNS(data: ICampaigns[]) {
    this.all = data;
  }

  @Action
  public getCampaign(id: number): ICampaigns | undefined {
    const found = this.all.findIndex(c => c.id === id);
    if (found !== -1) return this.all[found];
    return undefined;
  }

  @Action
  public setup(): Promise<void> {
    return new Promise((resolve, reject) => {
      getCampaigns()
        .then((res: any) => {
          this.SET_CAMPAIGNS(res || []);
          resolve();
        })
        .catch((e: any) => reject(e));
    });
  }
}

export const CampaignsModule = getModule(Campaigns);