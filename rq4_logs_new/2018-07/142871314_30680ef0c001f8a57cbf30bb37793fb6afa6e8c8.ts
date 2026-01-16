// TO DO

interface Channel {
    sendToQueue(queueName, buffer, config?): void
}

export const ch: Channel = {
    sendToQueue: () => 'not implemented'
}
