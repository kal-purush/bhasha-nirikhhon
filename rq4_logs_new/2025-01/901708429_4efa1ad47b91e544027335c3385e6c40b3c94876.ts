import { MailerService } from '@/mailer/mailer.service';
import { OnWorkerEvent, Processor, WorkerHost } from '@nestjs/bullmq';
import { Job } from 'bullmq';
import { expiredShortCodeMailTemplate } from '@/template/email.template';
import { ShortUrlsService } from '@/short-urls/short-urls.service';
import { LoggerService } from '@/logger/logger.service';
import { errorMessage } from '@/common/messages';

@Processor('notifyExpiredUrl')
export class ExpiryEmailConsumer extends WorkerHost {
	constructor(
		private readonly mailerService: MailerService,
		private readonly shortUrlService: ShortUrlsService,
		private readonly logger: LoggerService,
	) {
		super();
	}
	async process(job: Job): Promise<void> {
		const emailData = {
			subject: expiredShortCodeMailTemplate.subject,
			to: [{ name: job.data.name, address: job.data.email }],
			html: expiredShortCodeMailTemplate.body(job.data.shortCode, job.data.name),
			text: errorMessage.expiredEmailTempText,
		};
		await this.mailerService.sendEmail(emailData);
		await this.shortUrlService.deleteUrls(job.data.id);
		this.logger.log(`${job.data.shortCode} is expired and hence deleted`);
	}

	@OnWorkerEvent('active')
	onActive(job: Job): void {
		this.logger.log(`Processing job ${job.id} of type ${job.name} }`);
	}
	@OnWorkerEvent('completed')
	onComplete(job: Job): void {
		this.logger.log(`Job with id ${job.id} of type ${job.name} has been completed`);
	}

	@OnWorkerEvent('failed')
	async onFailed(job: Job, err: Error): Promise<void> {
		this.logger.error(`${errorMessage.jobFailed} ${job.id} of type ${job.name} \n ${err.message}`);
		if (job.attemptsMade >= (job.opts.attempts || 5)) {
			this.logger.log(`${errorMessage.emailSendFailed} ${job.data.email}`);
			this.logger.log(`${errorMessage.jobRemoved} ${job.id}`);
			await this.shortUrlService.deleteUrls(job.data.id);
			job.remove();
		}
	}
}