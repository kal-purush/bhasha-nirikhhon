import { Request, Response } from "express";
import { QueryResult } from "pg";
import { pool } from '../database';
import { Poll } from "../models/Poll";
import { Answer } from "../models/Answer";
import { CreatePollRequest } from "../models/CreatePollRequest";
import { OptionVotes } from "../models/OptionVotes";
import { PollOptionsResults } from "../models/PollOptionsResults";
import { Option } from "../models/Option";
import { v4 as uuidv4 } from 'uuid';

export const getPolls = async (req: Request, res: Response): Promise<Response<Poll[]>> => {
    const response: QueryResult<Poll> = await pool.query<Poll, Poll[]>("SELECT * FROM \"public\".\"Polls\"");
    return res.status(200).json(response.rows)
}

export const getPollById = async (req: Request, res: Response): Promise<Response<Poll[]>> => {
    const response: QueryResult<Poll> = await pool.query<Poll, Poll[]>(`SELECT * FROM \"public\".\"Polls\" WHERE \"PollId\" = '${req.params.id}'`);
    return res.status(200).json(response.rows)
}

export const getByEvent = async (req: Request, res: Response): Promise<Response<Poll[]>> => {
    const response: QueryResult<Poll> = await pool.query<Poll, Poll[]>(`SELECT * FROM \"public\".\"Polls\" WHERE \"EventId\" = '${req.params.eventId}'`);
    return res.status(200).json(response.rows)
}

export const getResult = async (req: Request, res: Response): Promise<Response> => {
    const pollId = req.params.id;
    const responsePoll: QueryResult<Poll> = await pool.query<Poll, Poll[]>(`SELECT * FROM \"public\".\"Polls\" WHERE \"PollId\" = '${pollId}'`);
    const responseAnswer: QueryResult<OptionVotes> = await pool.query<OptionVotes, OptionVotes[]>(`SELECT \"public\".\"Options\".*, (select  Count(*) from \"public\".\"Answers\" WHERE \"public\".\"Options\".\"OptionId\" = \"public\".\"Answers\".\"OptionId\") AS Votes
    FROM \"public\".\"Options\" 
    WHERE \"public\".\"Options\".\"PollId\" = '${pollId}';`);

    const result = new PollOptionsResults();
    result.options = responseAnswer.rows;
    result.poll = responsePoll.rows[0];

    return res.status(200).json(result)
}

export const createPoll = async (req: Request, res: Response): Promise<Response> => {

    const request = new CreatePollRequest();
    request.answer1 = req.body.answer1;
    request.answer2 = req.body.answer2;
    request.answer3 = req.body.answer3;
    request.answer4 = req.body.answer4;
    request.question = req.body.question;
    request.eventId = req.body.eventId;

    const options: Option[] = []

    const poll = new Poll();
    poll.eventId = request.eventId;
    poll.question = request.question;
    poll.pollId = uuidv4();

    if (request.answer1 && request.answer1.length > 0) {
        const option = new Option()
        option.text = request.answer1;
        option.optionId = uuidv4();
        option.pollId = poll.pollId;
        options.push(option);
    }

    if (request.answer2 && request.answer2.length > 0) {
        const option = new Option()
        option.text = request.answer2;
        option.optionId = uuidv4();
        option.pollId = poll.pollId;
        options.push(option);
    }

    if (request.answer3 && request.answer3.length > 0) {
        const option = new Option()
        option.text = request.answer3;
        option.optionId = uuidv4();
        option.pollId = poll.pollId;
        options.push(option);
    }

    if (request.answer4 && request.answer4.length > 0) {
        const option = new Option()
        option.text = request.answer4;
        option.optionId = uuidv4();
        option.pollId = poll.pollId;
        options.push(option);
    }
    await pool.query(`INSERT INTO \"public\".\"Polls\" ("PollId", "Question", "EventId") VALUES ($1, $2, $3);`, [poll.pollId, poll.question, poll.eventId])

    for (const option of options) {
        await pool.query(`INSERT INTO \"public\".\"Options\" ("OptionId", "Text", "PollId") VALUES ($1,$2,$3);`, [option.optionId, option.text, option.pollId])
    }


    return res.status(200).json({ poll: poll, options: options })
}

export const vote = async (req: Request, res: Response): Promise<Response> => {
    await pool.query(`INSERT INTO \"public\".\"Answers\" (\"OptionId\") VALUES ('${req.params.optionId}')`)
    return res.status(200).json();
}

export const deletePoll = async (req: Request, res: Response): Promise<Response> => {

    await pool.query(`DELETE FROM \"public\".\"Options\" WHERE \"PollId\" = '${req.params.id}'`);
    await pool.query(`DELETE FROM \"public\".\"Polls\" WHERE \"PollId\" = '${req.params.id}'`);
    return res.status(200).json();
}