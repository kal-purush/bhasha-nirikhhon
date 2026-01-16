import { Logger, ValidationPipe } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { DocumentBuilder, SwaggerModule } from '@nestjs/swagger';
import { CommandRunner, DefaultCommand } from 'nest-commander';
import { AppModule } from 'src/app.module';

@DefaultCommand({ name: 'start', description: 'start the API' })
export class StartCommand extends CommandRunner {
  private readonly logger = new Logger(StartCommand.name);

  async run(): Promise<void> {
    const app = await NestFactory.create(AppModule);

    const config = new DocumentBuilder()
      .setTitle('NeverLate API')
      .setDescription('Neverlate calendar application API')
      .setVersion('0.1')
      .addBearerAuth()
      .build();

    const document = SwaggerModule.createDocument(app, config);

    SwaggerModule.setup('api', app, document);

    app.useGlobalPipes(new ValidationPipe({ transform: true, whitelist: true }));

    const PORT = 3000;
    await app.listen(PORT, () => {
      this.logger.log(`Server started on port ${PORT}`);
    });
  }
}