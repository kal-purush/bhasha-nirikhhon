import { 
  ValueTest 
} from '../../interfaces'

export interface Predicate { 
  ( arg:any ) : boolean 
}

export interface Longish<T> {
  length:T
}

export interface Range<T> {
  min?:T;
  max?:T;
}

export type ListValue = any[]


export type MatchParam<T> = T|ValueTest<T>