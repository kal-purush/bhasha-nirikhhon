import dotenv from 'dotenv';
dotenv.config();
import amqp from 'amqplib';
import prisma from '../app';

export class NotificationService {
  private channel: amqp.Channel | null = null;

  async sendProviderNotification(providerId: string, data: any) {
    await this.init();
    const notification = {
      providerId,
      data
    };
    await this.channel!.sendToQueue(
      'provider-notifications',
      Buffer.from(JSON.stringify(notification))
    );
  }

  async init() {
    if (!this.channel) {
      const connection = await amqp.connect(process.env.RABBITMQ_URL!);
      this.channel = await connection.createChannel();
      await this.channel.assertQueue('provider-notifications', { durable: false });
    }
  }

  async notifyProvidersOfNewService(service: any) {
    await this.init();
    // 1. Find all providers matching the service category
    const relevantProviders = await prisma.provider.findMany({
      where: {
        field: {
          name: service.serviceCategory
        }
      }
    });

    // 2. Send notification to each provider with full service info
    for (const provider of relevantProviders) {
      await this.sendProviderNotification(provider.id, {
        ...service,
        type: 'NEW_SERVICE_REQUEST',
        timestamp: new Date()
      });
    }
  }
}