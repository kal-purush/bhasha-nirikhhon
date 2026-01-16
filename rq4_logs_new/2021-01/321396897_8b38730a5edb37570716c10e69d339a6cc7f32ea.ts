import * as pj from 'projen';
import { TaimosCdkApp } from '../projects';

export interface GraphQlApiAspectOptions {
  /**
   * The relative file name of the GraphQL spec
   *
   * @default 'schema.graphql'
   */
  readonly schemaFile?: string;
  /**
   * The relative file name of the type definition file to generate
   *
   * @default './src/lambda/types.generated.ts'
   */
  readonly typesFile?: string;
}

export class GraphQlApiAspect extends pj.Component {

  constructor(app: TaimosCdkApp, options: GraphQlApiAspectOptions = {}) {
    super(app);

    app.addCdkDependency(
      '@aws-cdk/aws-appsync',
      '@aws-cdk/aws-certificatemanager',
      '@aws-cdk/aws-cloudwatch',
      '@aws-cdk/aws-dynamodb',
      '@aws-cdk/aws-iam',
      '@aws-cdk/aws-kms',
      '@aws-cdk/aws-lambda',
      '@aws-cdk/aws-lambda-nodejs',
      '@aws-cdk/aws-route53',
      '@aws-cdk/aws-route53-targets',
    );

    app.addDeps(
      '@taimos/lambda-toolbox',
      'aws-sdk',
      'uuid',
    );

    app.addDevDeps(
      '@types/aws-lambda',
      '@types/uuid',
      'esbuild',
      '@graphql-codegen/cli',
      '@graphql-codegen/typescript',
    );

    app.addTask('generate:api', {
      exec: 'graphql-codegen',
      category: pj.tasks.TaskCategory.BUILD,
      description: 'Generate Types from GraphQL specification',
    });
    // TODO prepend to build task

    const codegenConfig = {
      schema: options.schemaFile ?? 'schema.graphql',
      config: {
        scalars: {
          AWSDate: 'string',
          AWSURL: 'string',
        },
      },
      generates: {} as any,
    };
    codegenConfig.generates[options.typesFile ?? './src/lambda/types.generated.ts'] = { plugins: ['typescript'] };

    new pj.YamlFile(app, 'codegen.yml', {
      obj: codegenConfig,
    });
  }

}