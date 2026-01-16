import { BaseEndpoint } from '../BaseEndpoint'

export class Projects extends BaseEndpoint {
  async createProject(project: Project_NEW) {
    try {
      const res = await this.axios.performRequest('POST', '/projects', {
        data: { project: project }
      })

      return this.respond(res)
    } catch (err) {
      return err
    }
  }

  async getProjectDetails(projectId: number): Promise<Project_GET_RES> {
    try {
      const res = await this.axios.performRequest(
        'GET',
        `/projects/${projectId}`
      )

      return this.respond(res)
    } catch (err) {
      return err
    }
  }
}

/** The object to send when creating a new Project. */
export interface Project_NEW {
  name: string
  wid: number
  template_id?: number
  is_private?: boolean
  cid?: number
}

/** The object returned by the API when a Project is created. */
export interface Project_NEW_RES {
  data: {
    id: number
    wid: number
    cid: number
    name: string
    billable: boolean
    is_private: boolean
    active: boolean
    at: string
    template_id: number
    color: string
  }
}

export interface Project_GET_RES {
  id: number
  wid: number
  cid: number
  name: string
  billable: boolean
  is_private: boolean
  active: boolean
  at: string
  template: boolean
  color: string
}