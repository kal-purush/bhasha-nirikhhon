import { Controller, Get, Inject } from '@nestjs/common';
import { HealthService } from './health.service';
import { SERVICES, SharedLoggerService } from '@app/shared-modules';

@Controller('status')
export class HealthController {
  constructor(
    private readonly health: HealthService,
    @Inject(SERVICES.LOGGER_SERVICE)
    private readonly logger: SharedLoggerService,
  ) {}

  @Get('health')
  async getHealth() {
    this.logger.logInfo({ message: 'Received healthCheck request' });
    this.health.getHealth();
  }
}