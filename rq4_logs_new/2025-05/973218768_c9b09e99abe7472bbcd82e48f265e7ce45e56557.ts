import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { join } from 'path';
import { HandlebarsAdapter } from '@nestjs-modules/mailer/dist/adapters/handlebars.adapter';
import { MailerModule } from '@nestjs-modules/mailer';

import { EmailService } from './email.service';
import emailConfig from './email-config';

@Module({
  imports: [
    ConfigModule.forRoot({ load: [emailConfig] }),
    MailerModule.forRootAsync({
      imports: [ConfigModule],
      inject: [ConfigService],
      useFactory: (configService: ConfigService) => ({
        transport: {
          host: configService.get<string>('smtHost'),
          port: configService.get<number>('smtPort'),
          secure: configService.get<string>('smtPort') === '465',
          auth: {
            user: configService.get<string>('smtUser'),
            pass: configService.get<string>('smtPass'),
          },
        },
        defaults: {
          from: `"${configService.get<string>('emailFrom')}" <${configService.get<string>('emailFromAddress')}>`,
        },
        template: {
          // TODO:Change path for production
          dir: join(process.cwd(), 'src', 'templates', 'emails'),
          adapter: new HandlebarsAdapter(),
          options: {
            strict: true,
          },
        },
      }),
    }),
  ],
  providers: [EmailService],
  exports: [EmailService],
})
export class EmailModule {}