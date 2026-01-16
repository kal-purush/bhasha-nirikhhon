import fp from 'fastify-plugin';

export default fp(
  async function http(fastify, { routes }: { routes: Record<string, any> }) {
    for (const [iface, methods] of Object.entries(routes)) {
      for (const [method, handler] of Object.entries(methods)) {
        if (typeof handler !== 'function') {
          continue;
        }

        fastify.route({
          method: 'POST',
          url: `/api/${iface}/${method}`,
          schema: {
            body: fastify.getSchema(`api:${iface}:${method}:params`),
          },
          handler: async function (request) {
            const { body } = request;
            const handler = routes[iface][method];
            if (!handler) {
              throw new Error('Method not found');
            }
            return await handler(body);
          },
        });
      }
    }
  },
  {
    name: 'http',
    dependencies: ['api-schemas'],
  },
);