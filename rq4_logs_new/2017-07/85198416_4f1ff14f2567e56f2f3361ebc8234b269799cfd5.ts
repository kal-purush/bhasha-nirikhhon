import { Source } from "../../shared/sources/Source";
import SourceModel = AltJS.SourceModel;
import { reportActions } from "../actions/ReportActions";
import {ReportingFetcher} from "./ReportingFetcher";
import {ReportStoreState} from "../stores/ReportStore";

export class ReportSource extends Source<ReportStoreState> {

    fetchReports: () => SourceModel<string[]>;

    constructor() {
        super();
        this.fetchReports = () => this.doFetch(() => "/reports/", {
            loading: reportActions.beginFetchReports,
            success: reportActions.updateReports,
            isCached: s => s.reports && s.reports.length > 0
        });
    }
}