import { Test, TestingModule } from '@nestjs/testing';
import { getRepositoryToken } from '@nestjs/typeorm';

import { JobOffersController } from './job-offers.controller';
import { JobOffersService } from './job-offers.service';
import { CreateJobOfferCommand } from './dto/create-job-offer-command.dto';
import { JobOffer } from './entities/job-offer.entity';
import { mockRepository } from '../shared/test-utils';

describe('JobOffers Controller', () => {
    let module: TestingModule;
    let controller: JobOffersController;
    let jobOfferService: JobOffersService;

    beforeAll(async () => {
        module = await Test.createTestingModule({
            controllers: [
                JobOffersController,
            ],
            providers: [
                JobOffersService,
                { provide: getRepositoryToken(JobOffer), useValue: mockRepository(JobOffer) },
            ],
        }).compile();
        controller = module.get<JobOffersController>(JobOffersController);
        jobOfferService = module.get<JobOffersService>(JobOffersService);
    });

    it('should be defined', () => {
        expect(controller).toBeDefined();
    });

    describe('createJobOffer', () => {

        it('should call service and return created job offer', async () => {
            const createJobOfferCommand = {} as CreateJobOfferCommand;
            const createdJobOffer = {} as JobOffer;
            jest.spyOn(jobOfferService, 'createJobOffer').mockImplementation(() => createdJobOffer);

            const result = await controller.createJobOffer(createJobOfferCommand);

            expect(jobOfferService.createJobOffer).toHaveBeenCalledWith(createJobOfferCommand);
            expect(result).toEqual(createdJobOffer);
        });

    });

});