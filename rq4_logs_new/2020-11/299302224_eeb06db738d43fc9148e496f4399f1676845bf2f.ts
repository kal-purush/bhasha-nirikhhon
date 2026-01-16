import protoLoader from '@grpc/proto-loader';
import { FastifyPluginCallback } from 'fastify';
import fp from 'fastify-plugin';
import grpc from 'grpc';

const grpcServerPlugin: FastifyPluginCallback<GRPCServerOptions> = function (app, { host, port }, done) {
  const grpcServer = new grpc.Server();
  grpcServer.bind(`${host}:${port}`, grpc.ServerCredentials.createInsecure());
  const fastifyGrpc: FastifyGrpc = {
    addService: grpcServer.addService.bind(grpcServer),
    loadProto: (filename: string) =>
      grpc.loadPackageDefinition(
        protoLoader.loadSync(filename, {
          keepCase: true,
          longs: String,
          enums: String,
          arrays: true,
        }),
      ),
  };
  app.decorate('grpc', fastifyGrpc);
  app.addHook(
    'onReady',
    done => (grpcServer.start(), app.log.info(`grpc server listening at "${host}:${port}"`), done()),
  );
  app.addHook(
    'onClose',
    (app, done) => (app.log.info('shutting down grpc server'), grpcServer.tryShutdown(() => done())),
  );
  done();
};

export const grpcServer = fp(grpcServerPlugin);

interface FastifyGrpc {
  addService: grpc.Server['addService'];
  loadProto: (filename: string) => grpc.GrpcObject;
}

declare module 'fastify' {
  interface FastifyInstance {
    grpc: FastifyGrpc;
  }
}

interface GRPCServerOptions {
  host: string;
  port: number;
}