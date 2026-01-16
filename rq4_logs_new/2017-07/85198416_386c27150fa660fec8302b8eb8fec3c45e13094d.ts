import { alt } from "../../../main/shared/alt";
import { expect } from "chai";
import {reportActions} from "../../../main/report/actions/ReportActions";
import {reportStore} from "../../../main/report/stores/ReportStore";

describe("ReportStore", () => {
    beforeEach(() => alt.recycle());

    it("has reports reportActions.updateReports", () => {
        const reports = [ "testreport", "anothertestreport"];

        reportActions.updateReports(reports);

        expect(reportStore.getState().reports).to.eql(reports);
        expect(reportStore.getState().ready).to.be.true;
    });

    it("is blank after reportActions.beginFetchReports", () => {
        reportActions.updateReports([ "report1", "report2"]);
        reportActions.beginFetchReports();

        expect(reportStore.getState().reports).to.eql([]);
        expect(reportStore.getState().ready).to.be.false;
    });
});