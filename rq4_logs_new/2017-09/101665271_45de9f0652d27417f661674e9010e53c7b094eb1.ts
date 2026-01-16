import { APP_INITIALIZER, Provider } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { ConfigService, ConfigFile } from './config.service';
import 'rxjs/add/operator/toPromise';

const ConfigEndpoint = '/config.json';

export async function fetchConfig(http: HttpClient, configService: ConfigService) {
  try {
    console.trace(`Fetching config from endpoint '${ConfigEndpoint}'`);
    const config = await http.get<ConfigFile>(ConfigEndpoint).toPromise();

    console.trace(`Retrieved configuration`);
    configService.init(config);
  } catch (err) {
    console.error(
      `App could not start! Could not fetch configuration from endpoint '${ConfigEndpoint}'`,
      err
    );
    throw err;
  }
}

export function fetchConfigFactory(
  http: HttpClient,
  configService: ConfigService
): () => Promise<void> {
  return () => fetchConfig(http, configService);
}

export const ConfigInitializer: Provider = {
  provide: APP_INITIALIZER,
  useFactory: fetchConfigFactory,
  deps: [HttpClient, ConfigService],
  multi: true,
};