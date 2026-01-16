import { Configuration, DefaultApi, WorkspacesCue } from '@jbs/codegen/src';
import { isDev } from './constants';

const withResponse = { expectResponse: true } as const;

export class QLab {
  async createCue(workspaceId: string): Promise<WorkspacesCue[]> {
    return this.api
      .workspaceIdNew({ id: workspaceId })
      .then(() => this.getCueList(workspaceId));
  }
  async getCueList(workspaceId: string): Promise<WorkspacesCue[]> {
    return this.api
      .workspaceIdCueLists({ id: workspaceId, ...withResponse })
      .then((it) => it?.data?.[0]?.cues || []);
  }

  async getWorkspaceId(): Promise<string> {
    return this.api.workspaces(withResponse).then((w) => w.data?.[0].uniqueID);
  }

  async go(cueNumber: string) {
    return this.api.cueCueNumberGo({ cueNumber });
  }
  async stop(cueNumber: string) {
    return this.api.cueCueNumberStop({ cueNumber });
  }

  private api: DefaultApi;
  constructor(window: { location: { host; port } }) {
    const port = isDev ? 5000 : window.location.port;
    this.api = new DefaultApi(
      new Configuration({
        basePath: `http://${window.location.hostname}:${port}/api`,
      })
    );
  }
}