/**
 * Handles incoming messages in a queue and processes them based on their
 * message type.
 * @param batch - The batch of messages to process.
 * @param env - The environment context for the handler.
 * @param ctx - The execution context for managing async operations.
 * @returns A promise that resolves when all messages in the batch have been
 *   processed and acknowledged.
 */
export async function queueHandler(
  batch: MessageBatch,
  env: Env,
  ctx: ExecutionContext,
) {
  for (const message of batch.messages) {
    // @ts-expect-error: A message body type might not have a clear structure
    const { type, data } = message.body

    switch (type) {
      case 'events':
        try {
          return
          message.ack()
        } catch (error) {
          console.error(error)
          message.retry()
        }
        break
      default:
        console.log(`No handler for the queue type: ${String(type)}`)
    }
  }
}