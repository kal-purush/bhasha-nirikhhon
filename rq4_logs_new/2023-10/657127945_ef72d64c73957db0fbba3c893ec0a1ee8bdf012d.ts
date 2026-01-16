import { Request, Response } from "express";
import { ProductRepository } from "../repository/ProductRepository";
import { SalesOrderRepository } from "../repository/SalesOrderRepository";
import { UserRepository } from "../repository/UserRepository";
import { AdminService } from "../services/admin.service";
import { ProductService } from "../services/product.service";
import SalesService from "../services/sales.service";

class AdminController{
    private SalesService: SalesService = new SalesService();
    private allProducts: ProductRepository = new ProductRepository();
    private allOrders: SalesOrderRepository = new SalesOrderRepository();
    private allUsers: UserRepository = new UserRepository();

    getDashboard = async (req:Request, res:Response)=>{
        const sales = await this.SalesService.FindAll();
        const products = await this.allProducts.FindAll();
        const orders = await this.allOrders.FindAll();
        return res.status(200).json({sales, products, orders});
    }

    getAnalytics = async (req:Request, res:Response)=>{
        const sales = await this.SalesService.FindAll();
        const products = await this.allProducts.FindAll();
        const orders = await this.allOrders.FindAll();
        return res.status(200).json({sales, products, orders});
    }
    getProducts = async (req:Request, res:Response)=>{
       
        const products = await this.allProducts.FindAll(); 
        return res.status(200).json({ products });
    }
    getOrders = async (req:Request, res:Response)=>{
         
        const orders = await this.allOrders.FindAll();
        return res.status(200).json({ orders});
    }

    getCustomers = async (req:Request, res:Response) => {
        const users = await this.allUsers.FindAll();

        return res.status(200).json({users})
    }
    getVendors = async (req:Request, res:Response) => {
        const users = await this.allUsers.FindAll();

        return res.status(200).json({users})
    }
    getIntegration = async (req:Request, res:Response) => {
         

        return res.status(200).json({
            "integration":[]
        })
    }
    getSettings = async (req:Request, res:Response) => {
         

        return res.status(200).json({
            "settings":[]
        })
    }
}

export default new AdminController();