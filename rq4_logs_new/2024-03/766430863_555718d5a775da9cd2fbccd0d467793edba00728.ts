import { DynamicModule, Module } from "@nestjs/common";
import {
  CAMPAY_CONFIG_OPTIONS,
  CampayModuleAsyncOptions,
  CampayModuleConfigOptions
} from "./types";
import { CampayService } from "./campay.service";

@Module({})
export class CampayModule {
  static forRoot(options: CampayModuleConfigOptions): DynamicModule {
    console.log("😥", options);
    return {
      module: CampayModule
    };
  }

  /**
   * Example:
   * ```typescript
   * 
   @Module({
      imports: [
        CampayModule.forRootAsync({
            useFactory: (config: Config): CampayModuleConfigOptions => {
              return {
                apiKey: config.API_KEY,
                isProduction: config.IS_PRODUCTION,
              };
            },
            inject: [Config],
            providers: [Config],
        }),
      ],
      controllers: [AppController],
      providers: [AppService, Config],
    })
    export class AppModule {}
   * ```
   */
  static forRootAsync(options: CampayModuleAsyncOptions): DynamicModule {
    return {
      module: CampayModule,
      imports: options.imports,
      providers: [
        CampayService,
        {
          provide: CAMPAY_CONFIG_OPTIONS,
          ...options
        },
        ...(options.providers ?? [])
      ]
    };
  }
}