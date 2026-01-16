const fs = require('fs').promises

class ProductManager {


    constructor() {
        this.path = "Products.json"
    }

    async addProduct(title, description, price, thumbnail, code, stock) {
        try {
            //Primero leemos el archivo para obtener los items del json
            const products = await this.getProducts();
            //Genero el id autogenerable con el largo del array
            const product_id = products.length + 1;
            //Metodo para buscar 
            const findProduct = products.find((product) => product.id === product_id);


            const product = {
                id: ++ProductManager.lastId,
                title,
                description,
                price,
                thumbnail,
                code,
                stock
            }

            //Agregamos el item nuevo en el listado
            products.push(prod);
            //Escribimos en el archivo nuevamente
            await fs.writeFile(this.path, JSON.stringify(products, null, 2))
            console.log("Producto creado con exito")

        } catch (error) {
            console.error("Error al crear producto", error);
        }
    }

    async getProducts() {
        try {
            //Leo el archivo
            const data = await fs.readFile(this.path, 'utf-8')
            //Tengo que transformar lo que me devuelve (texto) en un objeto
            return JSON.parse(data)
        } catch (error) {
            if (error.code === 'ENOENT') {
                return []
            } else {
                throw error
            }
        }
    }
    async getProductById(idProduct) {
        try {
            //Traigo todos los elementos del json con este metodo
            const products = await this.getProducts();
            //Busco el producto
            const findProduct = products.find((product) => product.id === idProduct)

            if (!findProduct) {
                console.log("Error no hay producto con ese id")
            } else {
                return findProduct;
            }
        } catch (error) {
            console.error("Error al buscar el producto por ID", error);
        }
    }

    //--------------------Nuevos Metodos----------------//
    async readFilesMio() {
        try {
            const res = await fs.readFile(this.path, 'utf-8')
            const arrayDeProductos = JSON.parse(res)
            return arrayDeProductos
        } catch (error) {
            console.log('Error al leer el archivo', error)
        }
    }

    async saveFilesMio(arrayProducts) {
        try {
            await fs.writeFile(this.path, JSON.stringify(arrayProducts, null, 2))
        } catch (error) {
            console.log('Error al guardar el archivo', error)
        }
    }

    async upDateProduct(id, productUpdate) {
        try {
            const arrayProducts = await this.readFilesMio()

            const index = arrayProducts.findIndex(item => item.id === id)
            if (index !== -1) {
                arrayProducts.splice(index, 1, productUpdate)
                await this.saveFilesMio(arrayProducts)
            } else {
                console.log('No se encontro el producto a actualizar')
            }

        } catch (error) {
            console.log('Error, no se pudo actualizar el producto', error)
        }
    }

    async deleteProduct(id) {
        try {
            const arrayProducts = await this.readFilesMio()
            const deleteUpDate = arrayProducts.filter(item => item.id != id)
            await this.saveFilesMio(deleteUpDate)

        } catch (error) {
            console.log('No se puede pudo borrar el producto')
        }
    }
}
//----------------------------------------------------------------------------------------------------//
//Test
const manager = new ProductManager('./Productos.json')
//manager.getProduct()


const product1 = {

    title: 'Loros Barranqueros Titular',
    description: 'Camiseta Titular 2024',
    price: 200,
    thumbnail: 'Sin Imagen',
    code: 'P001',
    stock: 25
}
//manager.addProduct(product1)

const product2 = {
    title: 'Loros Barranqueros Suplente',
    description: 'Camiseta Suplente 2024',
    price: 300,
    thumbnail: 'Sin Imagen',
    code: 'P002',
    stock: 35
}
//manager.addProduct(product2)

const product3 = {
    title: 'Loros Barranqueros Alternativa',
    description: 'Camiseta alternativa con error de campo al cual le falta el precio',
    //price: 300,
    thumbnail: 'Sin Imagen',
    code: 'abc124',
    stock: 35
}
async function testActualizar(id) {
    await manager.upDateProduct(id, product5)
    console.log(product5)
}


//testActualizar(1)

async function testBorrar(id) {
    await manager.deleteProduct(id)
}


// export.module = ProductManager
export default ProductManager
//testBorrar(1)
//manager.getProduct()