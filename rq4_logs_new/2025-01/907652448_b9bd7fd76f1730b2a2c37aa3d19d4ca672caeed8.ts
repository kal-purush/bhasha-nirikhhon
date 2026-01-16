import { GetStudyRecordByDateRepositoryInput } from '../i-repositories';

export class AddStudyRecordServiceInput {
  constructor(
    public readonly date: string,
    public readonly userId: number,
    public readonly totalDuration: number,
    public readonly title: string,
    public readonly duration: number,
    public readonly startAt: number,
    public readonly endAt: number,
    public readonly totalBreak: number,
  ) {}

  mapToGetStudyRecordByDateRepositoryInput() {
    return new GetStudyRecordByDateRepositoryInput(this.date, this.userId);
  }

  mapToStudy() {
    return {
      title: this.title,
      duration: this.duration,
      startAt: this.startAt,
      endAt: this.endAt,
      totalBreak: this.totalBreak,
    };
  }
}

export interface IAddStudyRecordService {
  execute(dto: AddStudyRecordServiceInput): Promise<void>;
}