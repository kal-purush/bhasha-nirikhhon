import {Iterator} from './IteratorInterface';

export interface Container <T>{
    getIterator(): Iterator<T>;
}