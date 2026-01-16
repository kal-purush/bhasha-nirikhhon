import { body } from "express-validator";
import Product from "../../../models/Product";
import { Ok } from "../../../utils/server/commonError";
import { logHandler } from "../../../utils/server/commonHandler";
import { validateRequest } from "../../../utils/server/middleware";

const handler = logHandler()
    // how to make restful api with different permission?
    .patch(
        validateRequest([body(["id"]).exists()]),
        async (req, res) => {
            const { id } = req.body
            await Product.updateOne({ id }, { $inc: { viewcount: 1 } })
            return Ok(res, "success")
        }
    )

export default handler