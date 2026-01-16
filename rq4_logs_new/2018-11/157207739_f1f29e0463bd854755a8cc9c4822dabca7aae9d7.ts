import { boardModel } from "../../../dbSchema/boardSchema";
import { indexModel } from "../../../dbSchema/indexSchema";

export class Board {
    constructor() {}

    setBoardIndex(boardIndex: number): Promise<any> {
        return new Promise(async (resolve, reject) => {
            await indexModel.create({model: 'board', modelIndex: boardIndex}, (err, result) => {
                if(err) reject("board.model.setBoardIndex() error");
                else resolve(result);
            });
        });
    }

    getBoardIndex(): Promise<any> {
        return new Promise(async (resolve, reject) => {
            await indexModel.findOne({model: 'board'}, (err, result) => {
                if (err) reject("getBoardIndex() error");
                else resolve(result);
            });
        });
    }

    incBoardIndex(boardIndex: number): Promise<void> {
        return new Promise(async (resolve, reject) => {
            await indexModel.findOneAndUpdate({model: 'board'}, {model: 'board', modelIndex: boardIndex}, {new: true}, (err, result) => {
                if(err) reject(err);
                else resolve(result);
            });
        });
    }

    createBoard(boardData: any): Promise<any> {
        return new Promise(async (resolve, reject) => {
            await boardModel.create(boardData, (err, result) => {
                if(err) reject(err);
                else resolve(result);
            })
        });
    }

    listBoard(): Promise<void> {
        return new Promise(async (resolve, reject) => {
           await boardModel.find({}, (err, results) => {
               if(err) reject(err);
               else resolve(results);
           })
        });
    }

    getBoard(boardIndex: number): Promise<any> {
        return new Promise(async (resolve, reject) => {
           await boardModel.findOne({boardIndex: boardIndex}, (err, result) => {
               if(err) reject(err);
               else resolve(result);
           })
        });
    }

    updateBoard(boardIndex: number, boardData: any): Promise<any> {
        return new Promise(async (resolve, reject) => {
            await boardModel.findOneAndUpdate({boardIndex: boardIndex}, boardData, {new: true}, (err, result) => {
                if(err) reject(err);
                else resolve(result);
            })
        });
    }

    deleteBoard(boardIndex: number): Promise<any> {
        return new Promise(async (resolve, reject) => {
           await boardModel.remove({boardIndex: boardIndex}, (err, result) => {
               if(err) reject(err);
               else resolve(result);
           })
        });
    }
}

export const board: Board = new Board();