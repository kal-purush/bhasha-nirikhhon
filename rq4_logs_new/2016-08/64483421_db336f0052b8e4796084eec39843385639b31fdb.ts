import { Injectable } from '@angular/core';
import { NgRedux } from 'ng2-redux';
import { IAppState } from '../store';
import { GiphyService } from '../services/giphy.service';

@Injectable()
export class GifDetailsActions {
  static GIF_DETAILS_LOADING = 'GIF_DETAILS_LOADING';
  static GIF_DETAILS_RECEIVED = 'GIF_DETAILS_RECEIVED';
  static GIF_DETAILS_ERROR = 'GIF_DETAILS_ERROR';

  constructor(private ngRedux: NgRedux<IAppState>, private giphyService: GiphyService ) {}

  loadingAction(id: string) {
    return {
      type: GifDetailsActions.GIF_DETAILS_LOADING,
      payload: { id }
    };
  }

  errorAction(error) {
    return {
      type: GifDetailsActions.GIF_DETAILS_ERROR,
      payload: error,
    };
  }

  successAction(results) {
    return {
      type: GifDetailsActions.GIF_DETAILS_RECEIVED,
      payload: {
        results: results.json().data,
      }
    };
  }

  getDetails(id: string) {
    this.ngRedux.dispatch(this.loadingAction(id));

    this.giphyService.getDetails(id).subscribe(
      result => this.ngRedux.dispatch(this.successAction(result)),
      error => this.ngRedux.dispatch(this.errorAction(error)));
  };
}