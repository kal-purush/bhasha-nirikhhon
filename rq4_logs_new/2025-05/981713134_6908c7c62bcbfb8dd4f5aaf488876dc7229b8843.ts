import { CommandHandler, EventBus, ICommandHandler } from '@nestjs/cqrs'
import { Logger } from '@nestjs/common'
import { OrderCompletedEvent } from '../../events/order-completed/order-completed.event'
import { ProcessOrderCommand } from './process-order.command'

@CommandHandler(ProcessOrderCommand)
export class ProcessOrderCommandHandler implements ICommandHandler<ProcessOrderCommand> {
	private readonly logger = new Logger(ProcessOrderCommandHandler.name)

	constructor(private readonly eventBus: EventBus) {}

	async execute(command: ProcessOrderCommand): Promise<void> {
		this.logger.debug(`Processing order with ID: ${command.orderId}`)

		try {
			// Here you'd implement the actual order processing logic
			// This is just a placeholder

			// After processing is complete, publish an event
			const orderCompletedEvent = new OrderCompletedEvent(
				command.orderId,
				command.id, // Using command ID as correlation ID
			)

			this.eventBus.publish(orderCompletedEvent)
			this.logger.log(`Order ${command.orderId} processed successfully`)
		} catch (error) {
			this.logger.error(`Failed to process order ${command.orderId}`, error.stack)
			throw error
		}
	}
}