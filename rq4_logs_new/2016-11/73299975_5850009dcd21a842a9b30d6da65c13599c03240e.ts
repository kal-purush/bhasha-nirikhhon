import * as Immutable from 'immutable';
// SpotifySong Presenter

export class SpotifySongPresenter {

  static fromCollection (collection: Immutable.List<any>): Immutable.List<Object> {
    return Immutable.List(collection.map( (item) =>  {
      return SpotifySongPresenter.present(item);
    }));
  }

  static present(item): Object {
    return {
      id: item.track.id,
      name: item.track.name,
      authorId: item.track.artists[0].id
    };
  }

}