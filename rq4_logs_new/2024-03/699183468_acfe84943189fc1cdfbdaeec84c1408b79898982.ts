export interface SurveyAnswer {
  id?: number
  survey_template_answer_id?: number
  survey_template_question_id?: number
}

export interface SurveyAnswers {
  survey_administration_id: number
  survey_answers: SurveyAnswer[]
}