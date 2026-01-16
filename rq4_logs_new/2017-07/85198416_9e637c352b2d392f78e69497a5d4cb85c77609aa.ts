import {FetchConfig, Source, UrlBuilder} from "../../shared/sources/Source";
import {makeNotificationException, notificationActions} from "../../shared/actions/NotificationActions";
import {ReportingFetcher} from "./ReportingFetcher";
import fetcher from "../../shared/sources/Fetcher";

export abstract class ReportingSource<TState> extends Source<TState> {

    protected doFetch<TModel>(urlFragment: UrlBuilder<TState>, config: FetchConfig<TState, TModel>): AltJS.SourceModel<TModel> {
        const handler = this.processResponse;
        const source: AltJS.SourceModel<TModel> = {
            remote(state: TState) {
                return handler((<ReportingFetcher>fetcher.fetcher).fetchFromReportingApi(urlFragment(state)))
                    .catch((error: any) => {
                        // Because of transpilation to ES5, we cannot test for instanceof NotificationException
                        if (error.hasOwnProperty("notification")) {
                            throw error;
                        } else if (error instanceof Error) {
                            throw makeNotificationException(error.message, "error");
                        } else {
                            throw makeNotificationException(error, "error");
                        }
                    });
            },
            local(state: TState): any {
                return Promise.resolve(true);
            },
            success: config.success,
            loading: config.loading,
            error: notificationActions.notify,
        };
        source.shouldFetch = ((state: TState) => !config.isCached(state)) as any;
        return source;
    }
}