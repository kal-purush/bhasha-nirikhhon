import { JobOffer } from './entities/job-offer.entity';
import { Specification } from '../specification.interface';
import { LocalDate } from 'js-joda';
import { FindConditions, MoreThan } from 'typeorm';

export class ActiveOfferSpecification implements Specification<JobOffer> {

    addToCriteria(criteria: FindConditions<JobOffer>): void {
        criteria.dateTo = MoreThan(LocalDate.now().toString());
    }

    test(subject: JobOffer): boolean {
        return LocalDate.now().isBefore(LocalDate.parse(subject.dateTo));
    }

}