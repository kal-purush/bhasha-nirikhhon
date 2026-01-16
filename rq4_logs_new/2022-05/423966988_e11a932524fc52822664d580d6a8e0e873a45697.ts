import { SwaggerModule, DocumentBuilder } from '@nestjs/swagger';
import { INestApplication } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';

export function configure(app: INestApplication): void {
  const config = app.get(ConfigService);
  const port = config.get<number>('port');

  const options = new DocumentBuilder()
    .setTitle('Gamify API')
    .setDescription('Gamify API description')
    .addServer(`http://localhost:${port}`, 'Localhost URL')
    .setVersion('0.1')
    .build();

  const document = SwaggerModule.createDocument(app, options);
  SwaggerModule.setup('docs', app, document);
}