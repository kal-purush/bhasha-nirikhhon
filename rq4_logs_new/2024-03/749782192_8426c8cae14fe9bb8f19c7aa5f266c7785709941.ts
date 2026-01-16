import { Module } from '@nestjs/common';
import { SOSController } from './sos/sos.controller';
import { TripsController } from './trips/trips.controller';
import { PrismaModule } from '../prisma/prisma.module';
import { SOSService } from './sos/sos.service';
import { SOSRepository } from './sos/sos.repository';
import { TripsService } from './trips/trips.service';
import { TripsRepository } from './trips/trips.repository';
import { TagsRepository } from '../tags/tags.repository';

@Module({
  imports: [PrismaModule],
  controllers: [SOSController, TripsController],
  providers: [
    SOSService,
    SOSRepository,
    TripsService,
    TripsRepository,
    TagsRepository,
  ],
  exports: [SOSService, SOSRepository, TripsService, TripsRepository],
})
export class BoardsModule {}