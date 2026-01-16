import { HttpModule, MiddlewareConsumer, Module, NestModule, RequestMethod } from "@nestjs/common";
import { ConfigModule, ConfigService } from "@nestjs/config";

import { OauthMiddleware } from "./oauth.middleware";

import { K8sServiceDNS } from "../common/lib/service";

@Module({
  imports: [
    HttpModule.registerAsync({
      imports: [ConfigModule],
      useFactory: (configService: ConfigService) => ({
        baseURL: K8sServiceDNS("oauth-server", configService.get("SERVER_PORT_OAUTH")),
      }),
      inject: [ConfigService],
    }),
  ],
  providers: [OauthMiddleware],
})
export class MiddlewareModule implements NestModule {
  configure(consumer: MiddlewareConsumer) {
    consumer.apply(OauthMiddleware).forRoutes({ path: "/todo/courses", method: RequestMethod.POST });
  }
}