import EvaluationModel from "../models/Evaluation.model";

export const getAllEvaluations = async () => {
    const evaluations = await EvaluationModel.findAll();
    return evaluations;
}

export const getOneEvaluation = async (id) => {
    const evaluation = await EvaluationModel.findByPk(id);
    return evaluation;
}

export const createEvaluation = async (data) => {
    const evaluation = await EvaluationModel.create(data);
    return evaluation;
}

export const updateEvaluation = async (id, data) => {
    const updatedEvaluation = await EvaluationModel.updateByIdAndUpdate(id, data);
    return updatedEvaluation;
}