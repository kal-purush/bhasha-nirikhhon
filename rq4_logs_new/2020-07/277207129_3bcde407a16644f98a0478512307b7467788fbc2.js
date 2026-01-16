const productsModel = require('../Models/ProductsModel');

let controllerProducts = {
    //Permite consultar los productos por su ID
    getproduct: async (req,res) =>{
        try {
            const id = req.params.id;
            const product = await productsModel.find({_id: id});

            if(product){
                return res.status(200).json({
                        data: product,
                        message: "Successful product inquiry"
                });
            }
        } catch (e) {
            return res.status(400).json({
                data: [],
                message: e.message
            });
        }  
    },
    //Permite agregar un nuevo producto
    addproduct: async (req, res) =>{
        try {
            const {name, price, image} = req.body;
            const newProduct = new productsModel({name, price, image});
            const result = await newProduct.save();

            if(result){
                return res.status(200).json({
                    data: [],
                    message: "successfully added product"
                });       
            }
        } catch (e) {
            return res.status(400).json({
                data: [],
                message: e.message
            });
        }
    },
    //Permite actualizar los datos de un producto
    updateproduct: async (req,res) =>{
        try {
            const {id, name, price, image} = req.body;
            const result = await productsModel.findByIdAndUpdate(id,{name, price, image});

            if(result){
                return res.status(200).json({
                    data: [],
                    message: "Product updated successfully"
                });
            }
        } catch (e) {
            return res.status(400).json({
                data: [],
                message: e.message
            });
        }
    },
    //Permite eliminar un producto
    deleteproduct: async (req,res) =>{
        try {
            const id = req.params.id;
            const result = await productsModel.findByIdAndDelete(id);

            if(result){
                return res.status(200).json({
                    data: [],
                    message: "product successfully removed"
                });
            }
        } catch (e) {
            return res.status(400).json({
                data: [],
                message: e.message
            });
        }
    },
    //Permite listar todos los productos
    listproducts: async (req,res) =>{
        try {
            const products = await productsModel.find({});
            //const result = await propertiesModel.find({}).count();

            if(products){
                return res.status(200).json({
                    data: products,
                    message: "Successful product consultation"
                });
            }
        } catch (e) {
            return res.status(400).json({
                data: [],
                message: e.message
            });
        }
    },
}

module.exports = controllerProducts;