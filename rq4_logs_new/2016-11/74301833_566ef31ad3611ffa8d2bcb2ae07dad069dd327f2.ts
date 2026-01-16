import {EpicJqlToCardWebModel} from "../converter/epicJqlToCardWebModel";
import {CardWebModel} from "../model/cardWebModel";
import {Dictionary} from "../commons/dictionary";
import {JqlModel} from "../model/jqlModel";
import {JqlService} from "./jqlService";
/**
 * Created by JJax on 23.11.2016.
 */

export class EpicService {
    private epicJqlToCardWebModel = new EpicJqlToCardWebModel();
    private jqlService = new JqlService();

    public async getEpicsWithParentId(httpClient) : Promise<Dictionary<CardWebModel[]>> {
        let epics = await this.getEpics(httpClient);
        return new Promise<any>((resolve, reject) => {
            var toReturn: Dictionary<CardWebModel[]> = {};
            for (let epic of epics.issues) {
                let projectId = epic.fields.project.id;
                if (!toReturn[projectId]) {
                    toReturn[projectId] = [];
                }
                toReturn[projectId].push(this.epicJqlToCardWebModel.apply(epic));
            }
            resolve(toReturn);
        });
    }

    private getEpics(httpClient) : Promise<any> {
        return this.jqlService.doRequest(this.prepareEpicJql(), httpClient);
    }

    private prepareEpicJql(): JqlModel {
        return {
            request: `"Epic Link" is EMPTY AND type not in subtaskIssueTypes()`,
            fields: [
                "name",
                "summary",
                "description",
                "project",
                "issuetype"
            ]
        };
    }
}