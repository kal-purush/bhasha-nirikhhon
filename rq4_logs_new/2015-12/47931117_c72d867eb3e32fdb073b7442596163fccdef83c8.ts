import { alt } from "../alt";
import { DiscogsArtistReleasesApiService } from './../services/DiscogsArtistReleasesApiService';

interface Actions {
    getReleases(artistId: number, page: number, itemsPerPage: number):void;
    setLoadingState(): void
}

// создатель событий приложения
class DiscogsArtistsReleasesActions implements AltJS.ActionsClass  {
    dispatch: ( ...payload:Array<any>) => void;

    // получить очередную страницу релизов
    getReleases(artistId: number, page: number, itemsPerPage: number) {
        this['actions'].setLoadingState();
        new DiscogsArtistReleasesApiService(artistId).call(page, itemsPerPage).then(
            (response) => {
                this.dispatch(response);
            }, (error) => {
                this.dispatch(error);
            }
        );
    }

    // установить состояние получения данных
    setLoadingState() {
        this.dispatch(true);
    }

}

export const darActions = alt.createActions<Actions>(DiscogsArtistsReleasesActions);


