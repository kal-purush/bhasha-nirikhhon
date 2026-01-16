import ProgressReporter from '.';
import { TestResults } from '@unional/test-progress-tracker';
import { noCoverage } from './testResultsExamples';
import a from 'assertron'
test('mark filtered if there is testNamePattern', () => {
  const subject = new ProgressReporter({ testNamePattern: 'a' } as jest.GlobalConfig)
  let actual: TestResults
  subject.appendTestResult = (_, results) => actual = results
  subject.onRunComplete(undefined, aggregateResult(noCoverage))

  a.satisfy(actual!, { ...noCoverage, duration: undefined, filtered: true })
})

test('mark filtered if there is testPathPattern', () => {
  const subject = new ProgressReporter({ testPathPattern: 'a' } as jest.GlobalConfig)
  let actual: TestResults
  subject.appendTestResult = (_, results) => actual = results
  subject.onRunComplete(undefined, aggregateResult(noCoverage))

  a.satisfy(actual!, { ...noCoverage, duration: undefined, filtered: true })
})

test('mark filtered if there is testNamePattern and testPathPattern', () => {
  const subject = new ProgressReporter({ testNamePattern: 'y', testPathPattern: 'a' } as jest.GlobalConfig)
  let actual: TestResults
  subject.appendTestResult = (_, results) => actual = results
  subject.onRunComplete(undefined, aggregateResult(noCoverage))

  a.satisfy(actual!, { ...noCoverage, duration: undefined, filtered: true })
})

test('not filtered if there is no testNamePattern or testPathPattern', () => {
  const subject = new ProgressReporter({} as jest.GlobalConfig)
  let actual: TestResults
  subject.appendTestResult = (_, results) => actual = results
  subject.onRunComplete(undefined, aggregateResult(noCoverage))

  a.satisfy(actual!, e => e.filtered === undefined)
})

test('no test will not append', () => {
  const subject = new ProgressReporter({} as jest.GlobalConfig)
  subject.appendTestResult = () => { throw new Error('should not call') }
  subject.onRunComplete(undefined, aggregateResult({} as any))
})

function aggregateResult(testResults: TestResults) {
  const result: jest.AggregatedResult = { ...testResults } as any
  if (testResults.coverage) {
    result.coverageMap = {
      getCoverageSummary() { return testResults.coverage }
    } as any
  }

  return result
}