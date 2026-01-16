
export * from './domain';
export * from './application';
export * from './infrastructure';
import { Router } from 'express';
import { ContactModel } from './infrastructure/persistence/mongoose/models';
import { MongoContactRepository } from './infrastructure/persistence/mongoose/repositories';
import { ContactController } from './infrastructure/http/controllers';
import { createContactRouter } from './infrastructure/http/routes';
import { CreateContactUseCase, GetContactUseCase, UpdateContactUseCase, DeleteContactUseCase, QueryContactUseCase } from './domain/use-cases';
import { logger } from '@app/utils/logger';

export function initContactModule(): Router {
    logger.info('Initializing Contact Module...');
    const contactRepository = new MongoContactRepository(ContactModel);
    const createUseCase = new CreateContactUseCase(contactRepository);
    const getUseCase = new GetContactUseCase(contactRepository);
    const updateUseCase = new UpdateContactUseCase(contactRepository);
    const deleteUseCase = new DeleteContactUseCase(contactRepository);
    const queryUseCase = new QueryContactUseCase(contactRepository);
    const controller = new ContactController(createUseCase, getUseCase, updateUseCase, deleteUseCase, queryUseCase);
    const router = createContactRouter(controller);
    logger.info('Contact Module initialized successfully');
    return router;
}