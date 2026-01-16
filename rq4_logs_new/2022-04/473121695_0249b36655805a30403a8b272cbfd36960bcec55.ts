import { cookie } from "express-validator"
import { NextApiRequest, NextApiResponse } from "next"
import { envExist } from "../validateEnv"
import { serverAuth, saveLog, validateRequest } from "./middleware"

interface authPath {
    path: string,
    method?: Array<string> // GET, HEAD, POST, PUT, DELETE, CONNECT, OPTIONS, TRACE, PATCH
}

const prefix = envExist(process.env.NEXT_PUBLIC_BASE_URL, "next public base url")
const ADMINPATH: Array<authPath> = []
const USERPATH: Array<authPath> = []


export default function authController(req: NextApiRequest, res: NextApiResponse) {
    const url = req.url
    const method = req.method
    ADMINPATH.forEach(value => {
        if (url.includes(prefix + value.path) && (!req.method || value.method.includes(method))) {
            return [serverAuth, saveLog, validateRequest([cookie("role").equals("admin")])]
        }
    })
    USERPATH.forEach(value => {
        if (url.includes(prefix + value.path) && (!req.method || value.method.includes(method))) {
            return [serverAuth, saveLog]
        }
    })
    return [saveLog]
}