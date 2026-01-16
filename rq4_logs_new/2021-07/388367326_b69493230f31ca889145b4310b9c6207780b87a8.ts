import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { CatModule } from './cat/cat.module';
import { GraphQLModule } from '@nestjs/graphql';
import { MongooseModule } from '@nestjs/mongoose';
import { loggerMiddleware } from './authorization.middleware';
import { UserModule } from './user/user.module';

@Module({
  imports: [
    CatModule,
    GraphQLModule.forRoot({
      autoSchemaFile: 'schema.gql',
      buildSchemaOptions: {
        fieldMiddleware: [loggerMiddleware],
      },
    }),
    MongooseModule.forRoot('mongodb://localhost/nest'),
    UserModule,
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}