import { makeAutoObservable } from "mobx";
import { runInAction } from "mobx/dist/internal";
import { getFavoriteTutors } from "../service/beWiselyService";

export class SearchStore {
  public loading: boolean = false
  public error = {
    message: '',
    status: false
  };
  constructor() {
    makeAutoObservable(this)
  }
  public favoriteTutors = []
  getFavoriteTutors = async (id: number) => {
    try {
      runInAction(() => (this.loading = true))
      const favorites = await getFavoriteTutors(id);
      runInAction(() => (this.favoriteTutors = favorites))
    } catch (e: any) {
      console.log(e)
      runInAction(() => (this.error.message = e.message))
      runInAction(() => (this.error.status = e.status))
    } finally {
      runInAction(() => (this.loading = false))
    }
  }
}