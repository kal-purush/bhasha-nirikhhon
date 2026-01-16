import { HereLayer } from '../models/here-layer.model';
import { BehaviorSubject } from 'rxjs';
import { Injectable } from '@angular/core';
import { LayerAction, LayerActions } from './actions';

@Injectable({ providedIn: 'root' })
export class LayerStore {
  private store: BehaviorSubject<HereLayer[]> = new BehaviorSubject([]);

  get state$() {
    return this.store.asObservable();
  }

  dispatch(action: LayerAction) {
    const state = this.reduce(this.store.value, action);

    this.store.next(state);
  }

  private reduce(state: HereLayer[], action: LayerAction): HereLayer[] {
    switch (action.type) {
      case LayerActions.AddLayer: {
        return [...state, action.payload];
      }
      case LayerActions.RemoveLayer: {
        const updatedState = state.filter(layer => layer !== action.payload);
        return [...updatedState];
      }

      default:
        return state;
    }
  }
}