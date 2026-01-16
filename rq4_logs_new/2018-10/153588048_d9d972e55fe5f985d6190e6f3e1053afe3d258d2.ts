export interface RuntimeOptions { }
export interface Options { }

export class ProgressReporter {
  options: RuntimeOptions
  constructor(public globalConfig: jest.GlobalConfig, jestOptions: Partial<Options>) {
    console.info('globalConfig', globalConfig)
    console.info('jestOptions', jestOptions)
  }
  onRunStart(results: jest.AggregatedResult, options: jest.ReporterOnStartOptions) {
    console.info('results', results)
    console.info('options', options)
  }
  onRunComplete(context, results: jest.AggregatedResult) {
    console.info('context', context)
    console.info('results', results)
  }
}