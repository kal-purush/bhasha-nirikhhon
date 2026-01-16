import { ServerDbClients } from '../types/server-db-clients.enum';
import {
    createSupabaseServerClient,
    createSupabaseServerComponentClient,
    createSupabaseServiceRoleClient,
} from './server-clients';

export const createServerDbClient = (flavour: ServerDbClients) => {
  switch (flavour) {
    case ServerDbClients.ServiceRole:
      return createSupabaseServiceRoleClient();
    case ServerDbClients.ServerComponent:
      return createSupabaseServerComponentClient();
    case ServerDbClients.Server:
    default:
      return createSupabaseServerClient();
  }
};

export { createSupabaseBrowserClient as createBrowserDbClient } from './browser-client';
export { createSupabaseMiddlewareClient as createMiddlewareDbClient } from './middleware-client';
