import {Product} from './product';

export interface Order {
    order: Product[],
    total: number
}