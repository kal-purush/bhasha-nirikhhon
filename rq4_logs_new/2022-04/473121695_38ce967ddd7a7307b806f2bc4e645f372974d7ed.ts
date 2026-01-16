import { body, cookie, query } from "express-validator";
import QABoard, { qaBoard } from "../../../models/QABoard";
import { Err, Ok } from "../../../utils/server/commonError";
import { customHandler, validate } from "../../../utils/server/commonHandler";
import { validateRequest } from "../../../utils/server/middleware";


const handler = customHandler()
    .get( //기능: 유저 qna info 불러오기, 입력: 없음, 출력:유저의 qna info
        async (req, res) => {
            const { userid } = req.cookies
            const result = await QABoard.find({ userid })
            return Ok(res, result)
        }
    )
    .post(//기능: 유저 qna 저장, 입력:userid, qacategory, title, content, 출력:저장 결과
        validateRequest([
            body("qacategory").isIn(["교환", "환불", "배송", "상품문의", "주문취소", "주문/결제", "이벤트"]),
            body("title").exists(),
            body("content").exists()]),
        async (req, res) => {
            const { userid } = req.cookies
            const { qacategory, title, content } = req.body
            const saveValue: qaBoard = { userid, qacategory, title, content, answer: false, date: new Date() }
            const result = await new QABoard(saveValue).save()
            return Ok(res, result)
        }
    )
    .put(  //기능: 유저 qna 수정, 입력:qaid, qacategory, title, content, userid, 출력:수정 결과
        validateRequest([
            body("qaid").isNumeric(),
            body("qacategory").isIn(["교환", "환불", "배송", "상품문의", "주문취소", "주문/결제", "이벤트"]),
            body("title").exists(),
            body("content").exists()]),
        async (req, res) => {
            const { userid } = req.cookies
            const { qaid, qacategory, title, content } = req.body
            const target: qaBoard | null = await QABoard.findOne({ userid, qaid })
            if (!target) {
                return Err(res, target)
            }
            const result = await QABoard.updateOne({ userid, qaid }, { $set: { qacategory, title, content, date: new Date() } })
            return Ok(res, result)
        }
    )
    .delete( //기능: 유저 qna 삭제, 입력:qaid, 출력:삭제 결과
        validateRequest([
            query("qaid").isNumeric()]),
        async (req, res) => {
            const { qaid } = req.query
            const target: qaBoard | null = await QABoard.findOne({ qaid })
            if (!target) {
                return Err(res, "qaboard not found")
            }
            await validate([cookie("userid").equals(target.userid)])(req, res)
            const result = await QABoard.deleteOne({ qaid })
            return Ok(res, result)
        }
    )

export default handler;