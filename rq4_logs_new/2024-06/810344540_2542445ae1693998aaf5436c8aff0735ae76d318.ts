import { Module } from '@nestjs/common';
import { GraphQLModule } from '@nestjs/graphql';
import { AuthModule } from './modules/auth/auth.module';
import { UserModule } from './modules/user/user.module';
import { PostModule } from './modules/blog/post.module';
import { ConfigModule } from '@nestjs/config';
import { DynamooseModule } from 'nestjs-dynamoose';
import { ApolloDriver } from '@nestjs/apollo';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      envFilePath: '.env',
    }),
    GraphQLModule.forRoot({
      driver: ApolloDriver,
      autoSchemaFile: true,
      context: ({ req }) => ({ req }),
      formatError: (error) => {
        const graphQLFormattedError = {
          message:
            error.extensions?.exception?.response?.message || error.message,
          statusCode:
            error.extensions?.exception?.response?.statusCode ||
            error.extensions?.code ||
            'INTERNAL_SERVER_ERROR',
        };
        return graphQLFormattedError;
      },
    }),
    DynamooseModule.forRoot({
      local: process.env.IS_DDB_LOCAL === 'true',
      aws: {
        region: process.env.AWS_PREFERRED_REGION,
      },
      table: {
        create: process.env.IS_DDB_LOCAL === 'true',
        prefix: process.env.DYNAMODB_TABLE_PREFIX || '',
        suffix: process.env.DYNAMODB_TABLE_SUFFIX || '',
      },
    }),
    UserModule,
    AuthModule,
    PostModule,
  ],
})
export class AppModule {}