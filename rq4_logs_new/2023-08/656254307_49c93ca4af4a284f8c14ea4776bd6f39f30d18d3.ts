import { HTTPTransport } from '@@shared/lib/HTTPTransport';
import { config } from '@@shared/lib/constants';
import { OpenAuthLogin } from '../model/types';

type ServiceIdResponse = { serviceId: string | null; error: Error | null };

class OpenAuthService extends HTTPTransport {
  constructor() {
    super('/', config.OAUTH_API_URL);
  }

  public async getServiceId(): Promise<ServiceIdResponse> {
    try {
      const result = await this.http.get(`service-id?redirect_uri=${config.OAUTH_CALLBACK_URL}`);

      const {
        data: { service_id },
      } = result;

      return { serviceId: service_id, error: null };
    } catch (error: unknown) {
      return { serviceId: null, error: error as Error };
    }
  }

  public async openAuthIn(data: OpenAuthLogin): Promise<void> {
    await this.http.post('', data);
  }
}

export const openAuthService = new OpenAuthService();