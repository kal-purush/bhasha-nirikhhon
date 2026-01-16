import AchievementEntity from '@domain/entities/AchievementEntity';
import { AchievementRepository } from '@infra/repositories/AchievementRepository';
import { TargetRepository } from '@infra/repositories/TargetRepository';

export default class CreateAchievementUseCase {
    constructor(
        private achievementRepository: AchievementRepository,
        private targetRepository: TargetRepository,
    ) {}

    async execute(input: Input) {
        const hasTarget = await this.targetRepository.hasTargetById(
            input?.targetId || '',
        );
        if (!hasTarget) throw new Error('Target not found');
        const achievement = AchievementEntity.create(
            input.month,
            input.value,
            input.targetId,
        );
        await this.achievementRepository.createAchievement(achievement);
        return {
            achievementId: achievement.getAchievementId().getValue(),
        };
    }
}

type Input = {
    month: number;
    value: number;
    targetId: string;
};