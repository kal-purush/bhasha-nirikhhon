import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { HydratedDocument, Types } from 'mongoose';
import { IScore, TermEnum } from 'src/shared/interfaces/schema.interface';
import * as mongoosePaginate from 'mongoose-paginate-v2';

export type ScoreDocument = HydratedDocument<Score>;

@Schema({ timestamps: true })
export class Score implements IScore {
  @Prop({ required: true, type: Types.ObjectId, ref: 'Student' })
  student: Types.ObjectId;

  @Prop({ required: true, type: Types.ObjectId, ref: 'ClassSubject' })
  classSubject: Types.ObjectId;

  @Prop({ required: true, type: Types.ObjectId, ref: 'AcademicYear' })
  academicYear: Types.ObjectId;

  @Prop({ required: true, type: String, enum: Object.values(TermEnum) })
  term: TermEnum; // Could be 'FIRST', 'SECOND'

  @Prop({ required: true, min: 0, max: 30 })
  CA: number;

  @Prop({ required: true, min: 0, max: 70 })
  exam: number;

  @Prop({ type: String }) // Optional field for remarks
  remarks?: string;

  // @Prop({ required: true, min: 0, max: 100 })
  // total: number;

  // A virtual field for the total score
  get total(): number {
    return this.CA + this.exam;
  }

  // Virtual field for grade calculation
  get grade(): string {
    const total = this.total;
    if (total >= 70) return 'A';
    if (total >= 60) return 'B';
    if (total >= 50) return 'C';
    if (total >= 45) return 'D';
    if (total >= 40) return 'E';
    return 'F';
  }
}

export const ScoreSchema = SchemaFactory.createForClass(Score);
// Define the virtual field in the schema
ScoreSchema.virtual('total').get(function (this: Score) {
  return this.CA + this.exam;
});

ScoreSchema.virtual('grade').get(function (this: Score) {
  const total = this.total;
  if (total >= 70) return 'A';
  if (total >= 60) return 'B';
  if (total >= 50) return 'C';
  if (total >= 45) return 'D';
  if (total >= 40) return 'E';
  return 'F';
});

// Ensure the virtuals are included when converting to JSON or Object
ScoreSchema.set('toJSON', { virtuals: true });
ScoreSchema.set('toObject', { virtuals: true });

ScoreSchema.plugin(mongoosePaginate);

ScoreSchema.index({
  classSubject: 1,
  academicYear: 1,
  term: 1,
});
ScoreSchema.index(
  {
    student: 1,
    classSubject: 1,
    academicYear: 1,
    term: 1,
  },
  { unique: true },
);

// // Virtual field for term average
// async termAverage(): Promise<number> {
//   const scores = await this.model('Score').find({ student: this.student, academicYear: this.academicYear, term: this.term }).exec();
//   const totalScore = scores.reduce((sum, score) => sum + (score.ca + score.exam), 0);
//   return scores.length ? totalScore / scores.length : 0;
// }