import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common'
import { MessagingConsumerService } from '~/libs/@core/kafka'
import { RedisService } from '~/libs/@core/redis'
import { NSGlobal } from '~/libs/common/enums/NSGlobal'
import { NSMessage } from '~/libs/common/enums/NSMessage'

@Injectable()
export class ReportConsumerMessage implements OnModuleInit {
	constructor(
		private readonly consumerService: MessagingConsumerService,
		@Inject(RedisService) private readonly redisService: RedisService,
	) {
		console.log('ReportConsumerMessage initialized')
	}
	async onModuleInit() {
		console.log('Initializing ReportConsumerMessage...')
		await this.consumerService.consume({
			topic: NSMessage.NSKafkaTopics.GENERATE_REPORT,
			groupId: 'report-consumer-group',
			onMessage: async message => {
				try {
					// Process the message
					// simulate processing the report message
					// await this.simulateTimeout((message as any).id)
					// console.log('Received report message:', message)
					// const parsedMessage = JSON.parse(message as string)
					this.redisService.set(
						`report:${(message as any).id}`,
						NSGlobal.EventStatus.Success,
						60 * 60 * 24, // Set expiration to 24 hours
					)
					console.log('Report status updated in Redis:', (message as any)?.id)
					// Here you can add your logic to handle the report message
				} catch (error) {
					// Handle any errors that occur during message processing
					// this.redisService.set(
					//   `report:${parsedMessage?.id}`,
					//   NSGlobal.EventStatus.Failed,
					//   60 * 60 * 24, // Set expiration to 24 hours
					// )
					console.error('Error processing report message:', error)
				}
			},
		})
	}
	async simulateTimeout(id?: string): Promise<void> {
		// Simulate a timeout scenario
		const delay = Math.random() * 10; // Random timeout between 0 and 1 seconds
		return new Promise<void>((resolve) => {
			setTimeout(() => {
				console.log('Simulated timeout occurred', id ? `for report ID: ${id}` : '')
				resolve()
			}, delay)
		})
	}
}