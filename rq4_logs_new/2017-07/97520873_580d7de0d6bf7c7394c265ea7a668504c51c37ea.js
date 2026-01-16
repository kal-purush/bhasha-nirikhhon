import { Server } from 'hapi';

export default {
  /**
   * Setup the hapi server for all tests
   *
   * @param plugins
   * @returns {Promise.<*>}
   */
  async hapi(plugins = []) {
    const server = new Server();
    server.connection();

    await server.register(plugins);

    return await server;
  },
};