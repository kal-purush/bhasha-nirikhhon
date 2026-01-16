import TargetEntity from '@domain/entities/TargetEntity';
import { TargetRepository } from '@infra/repositories/TargetRepository';

export default class GetTargetsUseCase {
    constructor(private readonly targetRepository: TargetRepository) {}

    async execute(input: Input): Promise<TargetEntity[]> {
        const targets: TargetEntity[] = await this.targetRepository.getTargets(
            input.accountId,
            input.offset,
            input.limit,
        );
        return targets;
    }
}

type Input = {
    accountId: string;
    limit: number;
    offset: number;
};