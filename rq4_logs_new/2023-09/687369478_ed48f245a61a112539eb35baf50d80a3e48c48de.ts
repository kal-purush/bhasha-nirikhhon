import { Request, Response, RequestHandler } from "express";
import { QuestionModel } from "../models/question";
import { CODE_BAD_REQUEST, CODE_INTERNAL_SERVER_ERROR, CODE_NOT_FOUND, hasKey, isQuestion } from "../utility";
import { ROUTE_QUESTIONS } from "../routes/questions";
import { isPartialQuestion } from "../utility/isPartialQuestion";

export const getQuestions: RequestHandler = async (
  req: Request,
  res: Response
) => {
  QuestionModel.find()
    .then(questions => res.json(questions))
    .catch((err) => {
      console.error(err);
      res.status(CODE_INTERNAL_SERVER_ERROR);
    }).finally(() => {
      res.end();
    });
};

export const getQuestion: RequestHandler = async (
  req: Request,
  res: Response
) => {
  const id = req.params.id;
  QuestionModel.findById(id)
    .then(question => {
      if (!question) {
        res.status(CODE_NOT_FOUND);
        return;
      }
      res.json(question);
    }).catch(err => {
      if (!hasKey(err, 'name') || err.name != 'CastError') {
        console.error(err);
        res.status(CODE_INTERNAL_SERVER_ERROR);
        return;
      }
      res.status(CODE_NOT_FOUND);
    }).finally(() => {
      res.end();
    });
};

export const createQuestion: RequestHandler = async (
  req: Request,
  res: Response
) => {
  const question = req.body;
  if (!isQuestion(question)) {
    res.status(CODE_BAD_REQUEST).end();
    return;
  }
  new QuestionModel(question).save()
    .then(question => {
      res.location(`${ROUTE_QUESTIONS}/${question.id}`);
    }).catch(err => {
      console.error(err);
      res.status(CODE_INTERNAL_SERVER_ERROR);
      return;
    }).finally(() => {
      res.end();
    });
};

export const updateQuestion: RequestHandler = async (
  req: Request,
  res: Response
) => {
  const question = req.body;
  if (!isPartialQuestion(question)) {
    res.status(CODE_BAD_REQUEST).end();
    return;
  }
  const id = req.params.id;
  QuestionModel.findByIdAndUpdate(id, question)
    .catch(err => {
      if (!hasKey(err, 'name') || err.name != 'CastError') {
        console.error(err);
        res.status(CODE_INTERNAL_SERVER_ERROR);
        return;
      }
      res.status(CODE_NOT_FOUND);
    }).finally(() => {
      res.end();
    });
};

export const deleteQuestion: RequestHandler = async (
  req: Request,
  res: Response
) => {
  const id = req.params.id;
  QuestionModel.findByIdAndDelete(id)
    .catch(err => {
      if (!hasKey(err, 'name') || err.name != 'CastError') {
        console.error(err);
        res.status(CODE_INTERNAL_SERVER_ERROR);
        return;
      }
    }).finally(() => {
      res.end();
    });
};