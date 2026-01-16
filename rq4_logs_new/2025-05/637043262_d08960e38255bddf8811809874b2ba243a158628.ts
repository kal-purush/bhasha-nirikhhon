import { CloudFormationClient } from '@aws-sdk/client-cloudformation'
import { LambdaClient } from '@aws-sdk/client-lambda'
import { updateLambdaCode } from '@bifravst/aws-cdk-lambda-helpers/util'
import chalk from 'chalk'
import { packBackendLambdas } from '../lambdas/packBackendLambdas.ts'

const cf = new CloudFormationClient()
const lambda = new LambdaClient()
const update = updateLambdaCode({ cf, lambda })

const start = new Date()
const packs = await packBackendLambdas()
console.debug('Packed lambdas in', new Date().getTime() - start.getTime(), 'ms')

await Promise.all(
	[`${process.env.STACK_NAME_PREFIX ?? 'teamstatus'}-backend`].map(
		async (stackName) =>
			update(stackName, packs, (arg, ...args) =>
				console.debug(chalk.blue(`[${stackName}]`), chalk.green(arg), ...args),
			),
	),
)

console.debug('Done')

console.debug(
	'Updated lambdas in',
	new Date().getTime() - start.getTime(),
	'ms',
)