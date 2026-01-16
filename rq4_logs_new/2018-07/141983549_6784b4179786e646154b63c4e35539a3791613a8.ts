import { Module } from '../../../node_modules/vuex';

export interface BaseState<S, R> {
    getState(): Module<S, R>;
}