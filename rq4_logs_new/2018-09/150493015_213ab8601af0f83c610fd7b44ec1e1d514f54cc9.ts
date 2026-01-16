import { Kinesis } from 'aws-sdk';

import { poll } from './poller';

export const runner = (lambda, _options) => {
    const options = _options || {};
    const kinesis = options.kinesis || new Kinesis({
        endpoint: process.env.KINESIS_ENDPOINT,
        region: 'ap-southeast-2',
        accessKeyId: 'FAKE',
        secretAccessKey: 'ALSO FAKE',
    });
    const streamName = options.streamName || process.env.STREAM_NAME;
    const log = options.console || console;

    const run = poll(kinesis, streamName, log);
    return run(lambda);
};