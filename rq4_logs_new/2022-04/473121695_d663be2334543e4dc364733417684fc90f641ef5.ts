import Product, { product } from "../../../models/Product";
import { Err, Ok } from "../../../utils/server/commonError";
import { customHandler } from "../../../utils/server/commonHandler";


async function sendByCategory(maxResults: number) {
    const categoryQuery = [{ "$group": { "_id": "category1", "categories": { "$addToSet": "$category1" } } }]
    const categories: String[] = (await Product.aggregate(categoryQuery).exec())[0]["categories"]
    const totalResult: product[] = []
    for (let category of categories) {
        const result = await Product.aggregate([{ "$match": { "category1": category } }, { "$sample": { "size": Math.floor(maxResults / categories.length) } }])
        totalResult.push(...result)
    }
    return totalResult
}

const handler = customHandler()
    .get(
        async (req, res) => {
            let { id, keyword, category1, category2, byCategory, sort, display, pagenum } = req.query
            const maxResults = Math.min(100, parseInt(display ? display.toString() : "10"))
            if (byCategory === "true") {
                const totalResult = await sendByCategory(maxResults)
                Ok(res, totalResult)
            } else {
                const totalQuery: Array<any> = []
                totalQuery.push(
                    { "$match": {} },
                )
                if (keyword) {
                    totalQuery.push({ "$match": { name: new RegExp(keyword?.toString(), "i") } })
                }
                if (id) {
                    totalQuery.push({ "$match": { id: parseInt(id?.toString()) } })
                }
                if (category1) {
                    totalQuery.push({ "$match": { category1 } })
                }
                if (category2) {
                    totalQuery.push({ "$match": { category2 } })
                }
                let sortQuery: string
                switch (sort) {
                    case "viewcount":
                        sortQuery = "-viewcount"
                        break
                    case "lowprice":
                        sortQuery = "price"
                        break
                    default:
                        sortQuery = "-price"
                }
                const parsedPagenum = pagenum ? parseInt(pagenum.toString()) : 1
                const result = await Product.aggregate(totalQuery).sort(sortQuery).limit(parsedPagenum * maxResults).skip((parsedPagenum - 1) * maxResults)
                const totalnum = (await Product.aggregate(totalQuery)).length
                Ok(res, { data: result, metadata: { totalnum } })
            }
        }
    )
    .post(
        async (req, res) => {
            const { id } = req.query
            const result = await Product.findOne({ id })
            if (!result) {
                const value = await new Product(req.body).save()
                Ok(res, value)
            } else {
                Err(res, result)
            }
        }
    )
    .put(
        async (req, res) => {
            const { id } = req.query
            const result = await Product.findOne({ id })
            if (result) {
                const value = await new Product(req.body).save()
                Ok(res, value)
            } else {
                Err(res, result)
            }
        }
    )
    .delete(
        async (req, res) => {
            Ok(res, "success")
        }
    )


export default handler;