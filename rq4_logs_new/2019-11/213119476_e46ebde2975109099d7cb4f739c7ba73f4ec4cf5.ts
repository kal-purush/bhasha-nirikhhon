import * as queryOptions from '../queryOptions';
import RequestHandler from '../request/handler';

export interface IQueryOptionIssue {
  issue: string[];
}

export default class Worklogs {
  private requestHandler: RequestHandler;

  constructor(requestHandler: RequestHandler) {
    this.requestHandler = requestHandler;
  }

  public async get(
    options?: Partial<
      IQueryOptionIssue &
        queryOptions.IDateRange &
        queryOptions.IUpdatedFrom &
        queryOptions.IPagination
    >
  ) {
    return await this.requestHandler.doRequest(
      this.requestHandler.makeRequestHeader(
        this.requestHandler.makeUri({
          pathname: `/worklogs`,
          query: options
        })
      )
    );
  }

  public async getWorklog(worklogId: string) {
    return await this.requestHandler.doRequest(
      this.requestHandler.makeRequestHeader(
        this.requestHandler.makeUri({
          pathname: `/worklogs/${worklogId}`
        })
      )
    );
  }

  public async getWorklogWorkAttributeValues(worklogId: string) {
    return await this.requestHandler.doRequest(
      this.requestHandler.makeRequestHeader(
        this.requestHandler.makeUri({
          pathname: `/worklogs/${worklogId}/work-attribute-values`
        })
      )
    );
  }

  public async getWorklogWorkAttributeValuesByKey(
    worklogId: string,
    key: string
  ) {
    return await this.requestHandler.doRequest(
      this.requestHandler.makeRequestHeader(
        this.requestHandler.makeUri({
          pathname: `/worklogs/${worklogId}/work-attribute-values/${key}`
        })
      )
    );
  }

  public async getForJiraWorklog(jiraWorklogId: string) {
    return await this.requestHandler.doRequest(
      this.requestHandler.makeRequestHeader(
        this.requestHandler.makeUri({
          pathname: `/worklogs/jira/${jiraWorklogId}`
        })
      )
    );
  }

  public async getForJiraFilter(
    jiraFilterId: string,
    options?: Partial<
      queryOptions.IDateRange &
        queryOptions.IUpdatedFrom &
        queryOptions.IPagination
    >
  ) {
    return await this.requestHandler.doRequest(
      this.requestHandler.makeRequestHeader(
        this.requestHandler.makeUri({
          pathname: `/worklogs/jira/filter/${jiraFilterId}`,
          query: options
        })
      )
    );
  }

  public async getForAccount(
    accountKey: string,
    options?: Partial<
      queryOptions.IDateRange &
        queryOptions.IUpdatedFrom &
        queryOptions.IPagination
    >
  ) {
    return await this.requestHandler.doRequest(
      this.requestHandler.makeRequestHeader(
        this.requestHandler.makeUri({
          pathname: `/worklogs/account/${accountKey}`,
          query: options
        })
      )
    );
  }

  public async getForProject(
    projectKey: string,
    options?: Partial<
      queryOptions.IDateRange &
        queryOptions.IUpdatedFrom &
        queryOptions.IPagination
    >
  ) {
    return await this.requestHandler.doRequest(
      this.requestHandler.makeRequestHeader(
        this.requestHandler.makeUri({
          pathname: `/worklogs/project/${projectKey}`,
          query: options
        })
      )
    );
  }

  public async getForTeam(
    teamId: string,
    options?: Partial<
      queryOptions.IDateRange &
        queryOptions.IUpdatedFrom &
        queryOptions.IPagination
    >
  ) {
    return await this.requestHandler.doRequest(
      this.requestHandler.makeRequestHeader(
        this.requestHandler.makeUri({
          pathname: `/worklogs/team/${teamId}`,
          query: options
        })
      )
    );
  }

  public async getForUser(
    accountId: string,
    options?: Partial<
      queryOptions.IDateRange &
        queryOptions.IUpdatedFrom &
        queryOptions.IPagination
    >
  ) {
    return await this.requestHandler.doRequest(
      this.requestHandler.makeRequestHeader(
        this.requestHandler.makeUri({
          pathname: `/worklogs/user/${accountId}`,
          query: options
        })
      )
    );
  }
}