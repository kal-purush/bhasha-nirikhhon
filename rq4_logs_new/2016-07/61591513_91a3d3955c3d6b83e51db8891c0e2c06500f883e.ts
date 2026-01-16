import {Injectable} from "@angular/core";
import {NavController, Loading, LoadingOptions} from "ionic-angular/index";
import {LoadingStatus} from "../loading-status/loading-status";

@Injectable()
export class LoadingStatusService {

    private loadingOptions: LoadingOptions;

    constructor() {
        this.loadingOptions = {};
    }

    public start = (nav: NavController): LoadingStatus => {

        const loadingStatus: LoadingStatus = new LoadingStatus();

        loadingStatus.loading = Loading.create(
            this.loadingOptions
        );

        loadingStatus.displayPromise = nav.present(
            loadingStatus.loading
        );

        return loadingStatus;
    };

}