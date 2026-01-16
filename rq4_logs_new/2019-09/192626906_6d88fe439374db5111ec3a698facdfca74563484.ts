import {Compra} from '../models/compra';
import {Producto} from '../models/producto';
import { MongoClient, ObjectId } from 'mongodb';
import { Request, Response } from 'express';
import { request } from 'https';
import {ProductoCompra} from '../models/producto_compra';
import {CompraDto} from '../models/compradto';
export class CompraController {
    private nombredb: string;
    private nombrecoleccion= "Productos";
    private nombrecoleccion3= "Compras";
    private conexión: MongoClient;
    constructor (conexión: MongoClient, nombredb:string) {
        this.conexión= conexión;
        this.nombredb= nombredb;
        this.Crear= this.Crear.bind(this);
    }
    public async Crear(req: Request, res: Response) {
        const compra1: Compra = {
            fecha: new Date().toISOString(),
            comprafinalizada: false
        }
        const db = this.conexión.db(this.nombredb);
        const compras = db.collection(this.nombrecoleccion3);



        try {



            const compraNueva = await compras.insertOne(compra1);
            console.log("Compra agregada");
            res.json({
                mensaje: "Compra agregada a la base de datos",
                id: compraNueva.insertedId.toHexString(),
            });
        } catch (err) {
            console.error(err);
            res.status(500).json(err);
        }
    }
}