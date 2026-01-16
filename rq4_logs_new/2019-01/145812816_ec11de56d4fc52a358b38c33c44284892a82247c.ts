import {computed, observable} from "mobx";

import GameField from "../../GameField";

export default class GameLinkFormStore {
  public static readonly GAME_URL_PREFIX = '/game'

  @observable
  public radix: number

  @observable
  public rowSize: number

  @observable
  public initialSize: number

  @observable
  public seed: string

  public constructor(
    radix: number = GameField.RADIX_DEFAULT,
    rowSize: number = GameField.ROW_SIZE_DEFAULT(radix),
    initialSize: number = GameField.INITIAL_SIZE_DEFAULT(rowSize),
    seed: string = '',
  ) {
    this.radix = radix
    this.rowSize = rowSize
    this.initialSize = initialSize
    this.seed = seed
  }

  @computed
  public get gameUrl(): string {
    return (
      `${GameLinkFormStore.GAME_URL_PREFIX}` +
      `/radix/${this.radix}` +
      `/row/${this.rowSize}` +
      `/size/${this.initialSize}` +
      `${this.seed && '/seed/' + this.seed}/`
    )
  }
}