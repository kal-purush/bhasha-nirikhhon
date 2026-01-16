import { DataSource as ApolloDataSource } from 'apollo-datasource';

import { Node } from './generated';
import { Store } from "./store";


export interface DataSourceFactory<T> extends Function {
  (store: Store): DataSource<T>
}

// standard interface for getting/writing db record's, regardless of Model T
export interface DataSource<T> extends ApolloDataSource {
  // read
  byId?: ({id}: Pick<Node, 'id'>) => T,
  getAll?: () => T[],
  // write
  new?: (data: Omit<T, 'id' | 'dateCreated'>) => T
}

// all possible data-sources by key:
// note: if you're adding a new data-source to api/..../data-sources.ts, please add the key here:
export type DataSourcesKeys = 'plant' |
                              'species' |
                              'photo' |
                              'plantStage' |
                              'lightBulbTemplate' |
                              'environment' |
                              'eventsTargets' |
                              'waterEventData' |
                              'plantStageEventData' |
                              'temperatureEventData' |
                              'humidityEventData' |
                              'pHEventData' |
                              'eCEventData' |
                              'lightEventData';
  // todo future ? Omit<keyof ResolversTypes, 'Timestamp'|'SortDirection'|...> then change casing of all dataSources -> DataSources

// DataSources type used in the main gardn app
export type DataSources<T = any> = Record<DataSourcesKeys, DataSource<T>>