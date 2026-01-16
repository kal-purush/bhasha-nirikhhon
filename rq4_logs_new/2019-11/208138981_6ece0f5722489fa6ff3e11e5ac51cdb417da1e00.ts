import * as mongoose from 'mongoose';

import { ProblemType } from '@shared/index';

// every operand has a range of values it can be
export interface IRange extends mongoose.Types.Subdocument {
    lowerBound: number;
    upperBound: number;
}

export const RangeSchema = new mongoose.Schema({
    lowerBound: {
        required: true,
        type: Number,
    },
    upperBound: {
        required: true,
        type: Number,
    },
});
/**
 * multiplesOf defines the multiples the operands can be of. For example,
 * if it's set to 5 then operands can only be 5, 10, 15, 20, etc
 *
 * operands will contain at least 2 ranges (one for each required operand).
 * When determining the values for the problem a random number between the
 * range will be selected
 *
 * optExtraOperands defines whether or not the problem can have extra operands.
 * For example if this is set to 1 and operands only contains 2 ranges then
 * operand1 will be between the range specified in operands[0], operand2 will
 * be between the range specified in operands[1] and operand3 will also be
 * between the range specified in operands[1]. The extra operands all take their
 * definition from the last specified operand range in operands
 */
export interface IArithmeticProblemDefinition
    extends mongoose.Types.Subdocument {
    multiplesOf: number;
    operands: IRange[];
    optExtraOperands: number;
}

export const ArithmeticProblemDefinitionSchema = new mongoose.Schema({
    multiplesOf: {
        required: true,
        type: Number,
    },
    operands: {
        required: true,
        type: [RangeSchema],
    },
    optExtraOperands: {
        required: true,
        type: Number,
    },
});

/**
 * difficultyMap will contain an entry for each appropriate difficulty tier. Problem
 * types that are introduced early will start at g1e, however problem types that are
 * introduced later like fractions will start at g3m
 *
 * operator is the symbol of the math operation
 */
export interface IArithmeticProblemTemplate extends mongoose.Document {
    difficultyMap: Map<string, IArithmeticProblemDefinition>;
    operator: string;
    problemType: ProblemType;
}

export const ArithmeticProblemTemplateSchema = new mongoose.Schema({
    difficultyMap: {
        of: ArithmeticProblemDefinitionSchema,
        required: true,
        type: Map,
    },
    operator: {
        required: true,
        type: String,
    },
    optExtraOperands: {
        required: true,
        type: Number,
    },
});

// TODO: by default mongoose only looks for lowercase collection names,
// there is a way to change this but its not a priority
export const ArithmeticProblemTemplateModel = mongoose.model<
    IArithmeticProblemTemplate
>('arithmeticproblemtemplates', ArithmeticProblemTemplateSchema);