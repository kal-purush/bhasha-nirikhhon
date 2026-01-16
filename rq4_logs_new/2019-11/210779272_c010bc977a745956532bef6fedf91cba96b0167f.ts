import {
    ICampaignEntity,
    IFileEntity,
    IBusinessEntity,
    IInternshipEntity,
    IInternshipTypeEntity,
    IMentoringPropositionEntity,
    IMentorEntity,
    IStudentEntity,
} from '../../declarations';

export function isInt(t: number): t is number {
    return !Number.isNaN(Number(t));
}

export function checkPartialBusiness(check: Partial<IBusinessEntity>): check is IBusinessEntity {
    return !!check.name && !!check.postalCode && !!check.city && !!check.country && !!check.address;
}

export function checkPartialCampaign(check: Partial<ICampaignEntity>): check is ICampaignEntity {
    return (
        check.name &&
        check.description &&
        check.semester &&
        check.maxProposition &&
        check.startAt !== undefined &&
        isInt(check.startAt) &&
        isInt(check.endAt) &&
        !!check.category
    );
}

export function checkPartialFile(check: Partial<IFileEntity>): check is IFileEntity {
    return !!check.name && !!check.path && !!check.type;
}

export function checkPartialInternship(
    check: Partial<IInternshipEntity>,
): check is IInternshipEntity {
    return (
        check.subject &&
        check.description &&
        check.country &&
        check.city &&
        check.postalCode &&
        check.address &&
        check.additional &&
        check.state &&
        isInt(check.publishAt) &&
        isInt(check.startAt) &&
        isInt(check.endAt)
    );
}

export function checkPartialInternshipType(
    check: Partial<IInternshipTypeEntity>,
): check is IInternshipTypeEntity {
    return !!check.label;
}

export function checkPartialProposition(
    check: Partial<IMentoringPropositionEntity>,
): check is IMentoringPropositionEntity {
    return !!check.comment;
}

export function checkPartialMentor(check: Partial<IMentorEntity>): check is IMentorEntity {
    return !!check.firstName && !!check.lastName && !!check.email;
}

export function checkPartialStudent(check: Partial<IStudentEntity>): check is IStudentEntity {
    return !!check.firstName && !!check.lastName && !!check.email && !!check.semester;
}