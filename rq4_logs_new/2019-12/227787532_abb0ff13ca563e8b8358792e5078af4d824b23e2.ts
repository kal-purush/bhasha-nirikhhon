import { ModelInit, MutableModel, PersistentModelConstructor } from "@aws-amplify/datastore";



export declare class Hero {
  readonly id: string;
  readonly name: string;
  readonly power?: string;
  readonly status?: boolean;
  constructor(init: ModelInit<Hero>);
  static copyOf(source: Hero, mutator: (draft: MutableModel<Hero>) => MutableModel<Hero> | void): Hero;
}