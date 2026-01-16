import { Module } from '@nestjs/common';

import { RoutingModule } from './routes/routing.module';

@Module({
    imports: [RoutingModule],
})
export class AppModule {
    async onModuleInit() {
        const x = 4;
    }
}