import { Module } from '@nestjs/common';
import { SchemasModule } from '../schemas/schemas.module';
import { DatabaseModule } from '../database/database.module';
import { ConfigurationModule } from '../configuration/configuration.module';
import { MailModule } from '../mail/mail.module';
import { JwtModule } from '@nestjs/jwt';

@Module({
    imports: [
        SchemasModule,
        DatabaseModule,
        ConfigurationModule,
        MailModule,
        JwtModule,
    ],
    exports: [
        SchemasModule,
        DatabaseModule,
        ConfigurationModule,
        MailModule,
        JwtModule,
    ],
})
export class CommonModule {}