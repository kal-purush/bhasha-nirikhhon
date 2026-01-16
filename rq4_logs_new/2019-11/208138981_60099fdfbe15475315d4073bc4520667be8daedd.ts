import * as mongoose from 'mongoose';

import { ProblemType } from '@shared/index';
import { GeometricShape } from '@shared/models/problem';

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
 * cost is only applicable to certain difficulties in these cases area problem will have a
 * cost element in them. For example: Bob has a 2 meter by 3 meter garden and wants to put a fence
 * around it. The fencing costs $10 per meter. How much will it cost Bob to put fencing around their
 * garden?
 *
 * shapes is the different shapes to apply this geometry problem to. For example: square, rectangle, triangle
 *
 * sides provides the range of values the side lengths of the shape can be
 */
export interface IGeometryProblemDefinition extends mongoose.Types.Subdocument {
    cost: IRange;
    shapes: GeometricShape[];
    sides: IRange[];
}

export const GeometryProblemDefinitionSchema = new mongoose.Schema({
    cost: {
        required: true,
        type: RangeSchema,
    },
    shapes: {
        required: true,
        type: [String],
    },
    sides: {
        required: true,
        type: [RangeSchema],
    },
});

/**
 * difficultyMap will contain an entry for each appropriate difficulty tier. Problem
 * types that are introduced early will start at g1e, however problem types that are
 * introduced later like area will start at g5m
 */
export interface IGeometryProblemTemplate extends mongoose.Document {
    difficultyMap: Map<string, IGeometryProblemDefinition>;
    problemType: ProblemType;
}

export const GeometryProblemTemplateSchema = new mongoose.Schema({
    difficultyMap: {
        of: GeometryProblemDefinitionSchema,
        required: true,
        type: Map,
    },
    operator: {
        required: true,
        type: String,
    },
    problemType: {
        required: true,
        type: String,
    },
});

export const GeometryProblemTemplateModel = mongoose.model<
    IGeometryProblemTemplate
>('geometryproblemtemplates', GeometryProblemTemplateSchema);