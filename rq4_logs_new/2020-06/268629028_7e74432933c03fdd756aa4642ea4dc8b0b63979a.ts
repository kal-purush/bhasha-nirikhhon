import { BaseEndpoint } from '../BaseEndpoint'
import {
  Workspace,
  Workspace_UPDATE,
  Workspace_UPDATE_RES,
  WorkspaceUser,
  WorkspaceClient,
  WorkspaceGroup,
  WorkspaceProject,
  WorkspaceTask,
  WorkspaceTag
} from '../../interfaces/Workspace'

export class Workspaces extends BaseEndpoint {
  /**
   * Get data about all the workspaces where the token owner belongs to.
   *
   * TODO: Validate this function.
   */
  async getWorkspaces(): Promise<Workspace[]> {
    try {
      const res = await this.axios.performRequest('GET', '/workspaces')

      return this.respond(res)
    } catch (err) {
      return err
    }
  }

  /**
   * Gets the details of a single Workspace with the given ID.
   *
   * TODO: Validate this function.
   *
   * @param workspaceId The ID of the Workspace to retrieve.
   */
  async getWorkspace(workspaceId: number): Promise<Workspace> {
    try {
      const res = await this.axios.performRequest(
        'GET',
        `/workspaces/${workspaceId}`
      )

      return this.respond(res)
    } catch (err) {
      return err
    }
  }

  /**
   * Updates an existing Workspace with the given ID using the given update
   * object.
   *
   * TODO: Validate this function.
   *
   * @param workspaceId The ID of the Workspace to update.
   * @param workspace The update object containing the new properties.
   */
  async updateWorkspace(
    workspaceId: number,
    workspace: Workspace_UPDATE
  ): Promise<Workspace_UPDATE_RES> {
    try {
      const res = await this.axios.performRequest(
        'PUT',
        `/workspaces/${workspaceId}`,
        { data: { workspace: workspace } }
      )

      return this.respond(res)
    } catch (err) {
      return err
    }
  }

  /**
   * Gets the Users within a Workspace. The token owner must be a Workspace
   * admin for this request to succeed.
   *
   * TODO: Validate this function.
   *
   * @param workspaceId The ID of the Workspace to retrieve.
   */
  async getWorkspaceUsers(workspaceId: number): Promise<WorkspaceUser[]> {
    try {
      const res = await this.axios.performRequest(
        'GET',
        `/workspaces/${workspaceId}/users`
      )

      return this.respond(res)
    } catch (err) {
      return err
    }
  }

  /**
   * Gets the Users within a Workspace. The token owner must be a Workspace
   * admin for this request to succeed.
   *
   * TODO: Validate this function.
   *
   * @param workspaceId The ID of the Workspace to retrieve.
   */
  async getWorkspaceClients(workspaceId: number): Promise<WorkspaceClient[]> {
    try {
      const res = await this.axios.performRequest(
        'GET',
        `/workspaces/${workspaceId}/clients`
      )

      return this.respond(res)
    } catch (err) {
      return err
    }
  }

  /**
   * Gets the Groups within a Workspace. The token owner must be a Workspace
   * admin for this request to succeed.
   *
   * TODO: Validate this function.
   *
   * @param workspaceId The ID of the Workspace to retrieve.
   */
  async getWorkspaceGroups(workspaceId: number): Promise<WorkspaceGroup[]> {
    try {
      const res = await this.axios.performRequest(
        'GET',
        `/workspaces/${workspaceId}/groups`
      )

      return this.respond(res)
    } catch (err) {
      return err
    }
  }

  /**
   * Gets the Projects within a Workspace. The token owner must be a Workspace
   * admin for this request to succeed.
   *
   * TODO: Validate this function.
   *
   * @param workspaceId The ID of the Workspace to retrieve.
   * @param active The state of Projects to return (`true`/`false`/`both`)
   * @param actualHours Whether to return the completed hours per project.
   * @param onlyTemplates Whether to get only project templates.
   */
  async getWorkspaceProjects(
    workspaceId: number,
    active: string = 'true',
    actualHours: boolean = false,
    onlyTemplates: boolean = false
  ): Promise<WorkspaceProject[]> {
    try {
      const res = await this.axios.performRequest(
        'GET',
        `/workspaces/${workspaceId}/projects?active=${active}` +
          `&actual_hours=${actualHours}&only_templates=${onlyTemplates}`
      )

      return this.respond(res)
    } catch (err) {
      return err
    }
  }

  /**
   * Gets the Tasks within a Workspace. The token owner must be a Workspace
   * admin for this request to succeed.
   *
   * TODO: Validate this function.
   *
   * @param workspaceId The ID of the Workspace to retrieve.
   * @param active The state of Tasks to return (`true`/`false`/`both`)
   */
  async getWorkspaceTasks(
    workspaceId: number,
    active: string = 'true'
  ): Promise<WorkspaceTask[]> {
    try {
      const res = await this.axios.performRequest(
        'GET',
        `/workspaces/${workspaceId}/tasks?active=${active}`
      )

      return this.respond(res)
    } catch (err) {
      return err
    }
  }

  /**
   * Gets the Tags within a Workspace. The token owner must be a Workspace
   * admin for this request to succeed.
   *
   * TODO: Validate this function.
   *
   * @param workspaceId The ID of the Workspace to retrieve.
   */
  async getWorkspaceTags(workspaceId: number): Promise<WorkspaceTag[]> {
    try {
      const res = await this.axios.performRequest(
        'GET',
        `/workspaces/${workspaceId}/tags`
      )

      return this.respond(res)
    } catch (err) {
      return err
    }
  }
}