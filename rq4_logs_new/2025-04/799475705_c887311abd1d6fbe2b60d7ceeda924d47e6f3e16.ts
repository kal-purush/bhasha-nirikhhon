import { Injectable } from '@nestjs/common';
import { AsicsHttpBaseApiService } from '../../services/http-base.service';

@Injectable()
export class AsicsMiningApiService extends AsicsHttpBaseApiService {
  start(ip: string, token: string): Promise<void> {
    const url = this.buildUrl(ip, 'mining/start');

    return this.post(url, null, {
      headers: {
        Authorization: token,
      },
    });
  }

  stop(ip: string, token: string): Promise<void> {
    const url = this.buildUrl(ip, 'mining/stop');

    return this.post(url, null, {
      headers: {
        Authorization: token,
      },
    });
  }
}