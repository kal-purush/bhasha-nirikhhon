const LambdaStreamClient = require('./../index');

async function test() {
    const endPoint = 'ws://stream.luniverse.dev:80';
    const lChainId = `#insert lChainId#`;
    const accessKey = '#insert streamAccessKey#';

    try {
        let streamClient = new LambdaStreamClient(endPoint, lChainId, accessKey);

        streamClient.on('RECEIPT', async function message(messages) {
            for (let message of messages) {
                console.info(JSON.stringify(message));
            }

            let lastOffset = Number(messages[messages.length - 1].data.offset) + 1;

            await streamClient.commit(lastOffset);
        });

        streamClient.on('ERROR', async function message(message) {
            console.info(JSON.stringify(message));
        });

        await streamClient.start();
    } catch (err) {
        console.error(err);
    }
}

test().then();