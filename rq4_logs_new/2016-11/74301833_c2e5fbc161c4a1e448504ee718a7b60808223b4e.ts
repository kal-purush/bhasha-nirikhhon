import {JqlService} from "./jqlService";
import {JqlModel} from "../model/jqlModel";
/**
 * Created by JJax on 28.11.2016.
 */

export class IssueService {
    private jqlService = new JqlService();

    getIssuesForForIds(id: number[], epicCustomField: string, httpClient): Promise<any> {
        return new Promise<any>((resolve, reject) => {
            console.log(this.prepareJqlRequestForIssues(id, epicCustomField));

        });
        // jqlService.doRequest(, httpClient)
    }

    private prepareJqlRequestForIssues(id: number[], epicCustomField: string): JqlModel {
        return {
            request: `"Epic Link" in (` + id +`) OR parent in (` + id +`) ORDER BY created`,
            fields: [
                "summary",
                "description",
                "issuetype",
                "parent",
                epicCustomField
            ]
        };
    }
}