import {Order as OrderModel} from '../db-models/Order.ts';
import {OrderLot as OrderLotModel} from '../db-models/OrderLot.ts';
import {MongoModel} from '../classes/MongoModel.ts';
import * as q from 'q';
import config from '../../settings/index.ts';

export function registerOrder (productId: string, lotsIds: string[]): Promise<OrderLotModel> {
    var deferred = q.defer();
    var orderModel = new MongoModel(config.dbConfig.models.order.name);
    var orderLotModel = new MongoModel(config.dbConfig.models.orderLot.name);
    orderModel.insert({
        productId: productId
    }).then(function (order: OrderModel) {
        orderLotModel.insertMany(lotsIds.map(function (lotId: string) {
            var orderLot: OrderLotModel = {
                lotId: lotId,
                orderId: order.id
            }
            return orderLot;
        })).then(deferred.resolve).catch(deferred.reject);
    }).catch(deferred.reject);
    return deferred.promise;
}

export function getOrdersFromProductId (productId: string): Promise<OrderLotModel> {
    var deferred = Q.defer();
    var model = new MongoModel(config.dbConfig.models.orderLot.name);
    model.findAll({
        productId: productId
    }).then(deferred.resolve).catch(deferred.reject);
    return deferred.promise;
}