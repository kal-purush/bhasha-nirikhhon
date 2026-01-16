import * as mongoose from 'mongoose';
import { ObjectId } from 'bson';
import { ProblemType, ProblemDifficulty } from '@shared/models/problem';

export interface IProblemTypeProgress extends mongoose.Types.Subdocument {
    difficulty: ProblemDifficulty,
    currentDifficultyPoints: number,
    totalPoints: number
}

export const ProblemTypeSchema = new mongoose.Schema({
    difficulty: {
        type: ProblemDifficulty,
        required: true
    },
    currentDifficultyPoints: {
        type: Number,
        required: true
    },
    totalPoints: {
        type: Number,
        required: true
    }
});

export interface IMastery extends mongoose.Document {
    studentId: ObjectId;
    progress: Map<ProblemType, IProblemTypeProgress>;
}

export const MasterySchema = new mongoose.Schema({
    studentId: {
        type: String,
        required: true,
    },
    progress: {
        type: Map,
        of: ProblemTypeSchema,
        required: true,
    },
});

export const MasteryModel = mongoose.model<IMastery>('masteries', MasterySchema);