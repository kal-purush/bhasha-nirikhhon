import aTest, { TestInterface } from 'ava';

export interface ITestStreamData {
  id: string;
  data: object[];
  expectedResult: (offset: number) => object[];
}

export const test = aTest as TestInterface<{
  streams: ITestStreamData[];
}>;

const makeStreamData = (id: string, data: object[]): ITestStreamData => {
  const savedEvents = (offset: number) => {
    let eventId = offset;
    return data.map((v, i) => ({
      data: v,
      id: ++eventId,
      streamId: id,
      version: i + 1
    }));
  };

  return {
    data,
    id,
    expectedResult: savedEvents
  };
};

test.beforeEach(t => {
  const streams = [
    makeStreamData(
      'longStream',
      new Array(50).fill(undefined).map((_, i) => ({ value: i + 1 }))
    ),
    makeStreamData('stream1', [{ foo: 1 }, { bar: 2 }]),
    makeStreamData('stream2', [{ x: 1, y: 2 }, { x: 3, y: 4 }])
  ];

  t.context = { streams };
});