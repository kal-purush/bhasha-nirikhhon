import * as AWSMock from "aws-sdk-mock";
import * as AWS from "aws-sdk";
import * as mod from '../handler';
import * as jestPlugin from 'serverless-jest-plugin';
import mocks from '../mocks/test.data';

const event = {
  Records: mocks.product
}
// console.log("EVENT: [%o]", event);
const lambdaWrapper = jestPlugin.lambdaWrapper;
const wrapped = lambdaWrapper.wrap(mod, { handler: 'catalogBatchProcess' });
// console.log(wrapped)

describe('Lambda function [catalogBatchProcess]', () => {
  beforeAll((done) => {
    //  lambdaWrapper.init(liveFunction); // Run the deployed lambda

    done();
  });

  it('should run the [publish] function of the [SNS] service', () => {
    AWSMock.setSDKInstance(AWS);
    const publishMock = jest.fn((param) => { console.log(param) });
    AWSMock.mock('SNS', 'publish', publishMock(event))
    // AWSMock.mock('SNS', 'publish', 'MOCK')
    // console.log(wrapped)
    // return wrapped.run(event).then((response) => {
    //   console.log("RESPONSE [%o]", response)
    // expect(response).toBeDefined();
    // });
  });

  afterAll(() => {
    AWSMock.restore('SNS', 'publish');
  })
});