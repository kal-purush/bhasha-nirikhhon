import * as express from 'express';
import db from '../../db-schema';
import { formatGetSalesItemResponse } from './sales.helpers';

export const getAllSales = async (req: express.Request, res: express.Response) => {
  db.Sales.findAll().then((sales: any[]) => {
    const formatted: any[] = sales.map(formatGetSalesItemResponse);
    res.send(formatted);
  });
};

export const getSalesByCompanyName = async (req: express.Request, res: express.Response) => {
  db.Sales
    .findAll({
      where: {
        CompanyName: req.body.companyName,
      },
    })
    .then((sales: any[]) => {
      const formatted: any[] = sales.map(formatGetSalesItemResponse);
      res.send(formatted);
    });
};