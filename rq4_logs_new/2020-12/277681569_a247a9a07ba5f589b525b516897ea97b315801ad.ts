/**
 * Amplify를 이용한 프론트엔드 React 앱 배포 스택
 * Production, Test 환경에 대한 배포를 포함하고 있다.
 * @author hwasurr
 */
import * as cdk from '@aws-cdk/core';
import * as amplify from '@aws-cdk/aws-amplify';
import * as codebuild from '@aws-cdk/aws-codebuild';
import BaseStack from '../class/BaseStack';

const ID_PREFIX = 'Truepoint';
const DOMAIN = 'mytruepoint.com';
export class TruepointAmplify extends BaseStack {
  constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // *****************************************
    // react 앱 배포를 위한 Amplify Console
    // *****************************************
    const REPOSITORY_OWNER = 'whileTrueDev';
    const REPOSITORY_NAME = 'tp-mvp';
    const amplifyApp = new amplify.App(this, `${ID_PREFIX}AmplifyApp`, {
      appName: `${ID_PREFIX}Web`,
      sourceCodeProvider: new amplify.GitHubSourceCodeProvider({
        owner: REPOSITORY_OWNER,
        repository: REPOSITORY_NAME,
        oauthToken: cdk.SecretValue.secretsManager('Truepoint-AmplifyConsoleSecrets', {
          jsonField: 'AMPLIFY_GITHUB_ACCESS_TOKEN',
        }),
      }),
      buildSpec: codebuild.BuildSpec.fromObject({
        version: '1.0',
        frontend: {
          phases: {
            preBuild: {
              commands: ['yarn install', 'cd shared', 'yarn build'],
            },
            build: {
              commands: ['cd ../client/web', 'yarn build'],
            },
          },
          artifacts: {
            baseDirectory: 'client/web/build',
            files: ['**/*'],
          },
          cache: { paths: ['node_modules/**/*'] },
        },
      }),
      autoBranchDeletion: true,
    });

    // Add Branch
    // master 로 변경 필요!!
    // Add Domain
    const domain = amplifyApp.addDomain(DOMAIN);

    // master 환경
    const webBranch = amplifyApp.addBranch('master', {
      autoBuild: true,
      description: 'truepoint web production branch',
      // 환경 변수는 빌드할 때 앱에 필요한 상수 값이 포함된 키/값 페어입니다.
      // https://docs.aws.amazon.com/amplify/latest/userguide/environment-variables.html
      environmentVariables: { NODE_ENV: 'production' },
    });
    domain.mapRoot(webBranch);

    // Test 환경
    const testBranch = amplifyApp.addBranch('test', {
      autoBuild: true,
      description: 'truepoint web test branch',
      environmentVariables: { NODE_ENV: 'test' },
      basicAuth: amplify.BasicAuth.fromCredentials(
        'truepoint',
        cdk.SecretValue.secretsManager('Truepoint-AmplifyConsoleSecrets', {
          jsonField: 'AMPLIFY_TEST_PASSWORD',
        }),
      ),
    });
    domain.mapSubDomain(testBranch);

    // SPA 설정
    amplifyApp.addCustomRule(amplify.CustomRule.SINGLE_PAGE_APPLICATION_REDIRECT);
  }
}