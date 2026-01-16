const path = require("path")
const { deleteFileInPublic } = require("../../../utils/functions");
const { CreateProductSchema } = require("../../middleware/validator/admin/product.schema");
const Controller = require("../controllers");
const { ProductModel } = require("../../../models/products");
const createHttpError = require("http-errors");
const { ObjectIdValidator } = require("../../middleware/validator/public.validator");

class ProductController extends Controller {
    async add(req, res, next) {
        try {
            const productDataBody = await CreateProductSchema.validateAsync(req.body)
            if (typeof productDataBody.fileName == "string") {
                req.body.images = path.join(productDataBody.fileUploadPath, productDataBody.fileName).replaceAll("\\", "/")
            } else {
                req.body.images = productDataBody.fileName.map(img => {
                    return path.join(productDataBody.fileUploadPath, img).replaceAll("\\", "/")
                }) 
            }
            const {
                title, text, short_text, fileName, tags, category, fileUploadPath, 
                price, discount, count, type, weight, length, height, width,
            } = productDataBody
            const {images} = req.body
            const supplier = req.user._id
            let feature = {weight, length, height, width}
            if (!weight) feature.weight = 0
            if (!length) feature.length = 0
            if (!height) feature.height = 0
            if (!width) feature.width = 0

            const product = await ProductModel.create({
                title, text, short_text, fileName, tags, category, fileUploadPath, 
                price, discount, count, type, feature, images, supplier
            })

            res.status(200).send(product)
        } catch (error) {
            deleteFileInPublic(req.body.images)
            next(error)
        }
    }
    
    async edit(req, res, next) {
        try {
            
        } catch (error) {
            next(error)
        }
    }
    
    async remove(req, res, next) {
        try {
            const {productId} = req.params;
            await this.findProductById(productId)
            const product = await ProductModel.deleteOne({_id: productId})
            if (product.deletedCount == 0) throw createHttpError.BadRequest("product was not deleted")
            return res.status(200).send("deleted")
        } catch (error) {
            next(error)
        }
    }
    
    async getAll(req, res, next) {
        try {
            const {search} = req.query || ""
            const query = search ? {
                $text: {
                    $search: search
                },
            } : {}
            const product = await ProductModel.aggregate([
                {
                    $match: query
                },
                {
                    $lookup: {
                        from: "categories",
                        localField: "category",
                        foreignField: "_id",
                        as: "category"
                    }
                },
                {
                    $lookup: {
                        from: "users",
                        localField: "supplier",
                        foreignField: "_id",
                        as: "supplier"
                    }
                },
            ])

            return res.status(200).send(product)
        } catch (error) {
            next(error)
        }
    }
    
    async getOne(req, res, next) {
        try {
            const {productId} = req.params;
            const product = await this.findProductById(productId)
            return res.status(200).send(product)
        } catch (error) {
            next(error)
        }
    }

    async findProductById(productID) {
        const {id} = await ObjectIdValidator.validateAsync({id: productID})
        const product = await ProductModel.findById(id)
        if (!product) throw createHttpError.NotFound("product not found")
        return product;
    }
}


module.exports = {
    ProductController: new ProductController()
}