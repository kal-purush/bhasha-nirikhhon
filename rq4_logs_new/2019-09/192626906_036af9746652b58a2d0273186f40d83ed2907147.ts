import { Collection, Db } from "mongodb";
import {Producto} from '../models/producto';
export class ProductoService{
    private productos: Collection<any>;
    constructor (db:Db){
        this.productos=db.collection('Productos');
    }
    public CrearProducto (producto:Producto): Promise<void>{
        return new Promise(async(resolve,reject)=> {
            try{
                await this.productos.insertOne(producto);
                resolve();
            }catch(err){
                reject(err);
            }
        })
    }
}