import GenericResponse from "../model/dto/GenericResponse";
import Sprint, {ProjectPlan} from "../model/Sprint";
import CommonResponse from "../model/dto/CommonResponse";

/**
 *
 * @param projectId
 * @param workspaceId
 * @return list of sprints for current project
 */
export function getProjectPlan(projectId: string, workspaceId: string) {//TODO: implement
    return new Promise<GenericResponse<ProjectPlan>>((res, rej) => res(
        new GenericResponse(new ProjectPlan('My project',
            [new Sprint(new Date(), new Date(), 'To start', 'Excellent')]))));
}

/**
 *
 * @param projectId
 * @param workspaceId
 * @param sprint
 */
export function addSprint(projectId: string, workspaceId: string, sprint: Sprint) {
    return new Promise<CommonResponse>(resolve => resolve(new CommonResponse())); //TODO: implement
}