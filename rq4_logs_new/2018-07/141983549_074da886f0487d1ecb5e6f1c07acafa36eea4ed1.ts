import { GetterTree, MutationTree, ActionTree, Module } from 'vuex';
import { HeroStateModel } from './types';

export const state: HeroStateModel = {
  heroes: [
    { id: 11, name: 'Mr. Nice' },
    { id: 12, name: 'Narco' },
    { id: 13, name: 'Bombasto' },
    { id: 14, name: 'Celeritas' },
    { id: 15, name: 'Magneta' },
    { id: 16, name: 'RubberMan' },
    { id: 17, name: 'Dynama' },
    { id: 18, name: 'Dr IQ' },
    { id: 19, name: 'Magma' },
    { id: 20, name: 'Tornado' },
  ],
};

export const getters: GetterTree<HeroStateModel, any> = {
  allHeroes: (s) => s.heroes.filter(() => true),
};

export const heroState: Module<HeroStateModel, any> = {
  state,
  getters,
  namespaced: true,
};