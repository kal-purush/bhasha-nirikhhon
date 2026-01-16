/** The standard model of a Project. */
export interface Project {
  /** The name of the project (unique for client & workspace). */
  name: string

  /** The workspace ID for where the Project will be saved. */
  wid: number

  /** The Client ID. */
  cid?: number

  /** Whether the project is archived or not. */
  active: boolean

  /**
   * Whether the project is accessible for only project users or for all
   * workspace users.
   */
  is_private: boolean

  /** Whether the project can be used as a template. */
  template?: boolean

  /** The id of the template project used on current project's creation. */
  template_id?: number

  /**
   * Whether the project is billable or not (only available for pro workspaces).
   */
  billable?: boolean

  /**
   * Whether the estimated hours are automatically calculated based on task
   * estimations or manually fixed based on the value of `estimated_hours`.
   * (Premium Feature)
   */
  auto_estimates?: boolean

  /**
   * If `auto_estimates` is true, then the sum of task estimations is returned,
   * otherwise user inserted hours. (Premium Functionality)
   */
  estimated_hours?: number

  /**
   * Timestamp that is sent in the response for PUT, indicates the time the task
   * was last updated (read-only).
   */
  at: Date

  /** Id of the color selected for project. */
  color: number

  /** Hourly rate of the project. (Premium Functionality) */
  rate?: number

  /** Timestamp indicating when the project was created (UTC, read-only). */
  created_at: Date
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