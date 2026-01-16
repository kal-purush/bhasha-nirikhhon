import express, { Request, Response, NextFunction } from "express";
import { InvoiceGetFromClientType } from "@/types";
import { client } from "../db/dbConnnect.";

export const InvoiceCreate = function (
  _req: Request,
  res: Response,
  _next: NextFunction
) {
  try {
    const newInvoice: InvoiceGetFromClientType = _req?.body;

    client.query(
      `INSERT INTO invoice 
          (clientName, clientEmail, status, paymentDue, paymentTerms, description, total) 
          VALUES($1, $2, $3, $4, $5 ,$6, $7) RETURNING id`,
      [
        newInvoice?.clientName,
        newInvoice?.clientEmail,
        newInvoice?.status,
        newInvoice?.paymentDue,
        newInvoice?.paymentTerms,
        newInvoice?.projectDescription,
        newInvoice?.total,
      ],
      (err, results) => {
        if (err) {
          throw new Error(`${err?.name}: ${err?.message}`);
        }

        const invoiceId = results?.rows[0]?.id;

        // insert both address - if error delete reference
        // insert items - if error delete reference
        const invoiceValues = [
          [
            newInvoice?.clientAddress?.country,
            newInvoice?.clientAddress?.city,
            newInvoice?.clientAddress?.postCode,
            newInvoice?.clientAddress?.streetAddress,
            true,
            invoiceId,
          ],
          [
            newInvoice?.senderAddress?.country,
            newInvoice?.senderAddress?.city,
            newInvoice?.senderAddress?.postCode,
            newInvoice?.senderAddress?.streetAddress,
            false,
            invoiceId,
          ],
        ];

        for (let i = 0; i < invoiceValues?.length; i++) {
          client.query(
            `INSERT INTO address (country, city, postcode, street, client, invoice_id) VALUES ($1, $2, $3, $4, $5, $6)`,
            invoiceValues[i],
            (err, result) => {
              if (err) {
                // delete invoice here...
                throw new Error(`${err?.name}: ${err?.message}`);
              }
            }
          );
        }

        newInvoice?.items?.forEach((item) => {
          client.query(
            `INSERT INTO items (name, quantity, price, invoice_id) VALUES ($1, $2, $3, $4)`,
            [item?.itemName, item?.itemQuantity, item?.itemPrice, invoiceId],
            (err, result) => {
              if (err) {
                // delete invoice here
                throw new Error(`${err?.name}: ${err?.message}`);
              }
            }
          );
        });

        // pg closes client automatically
        return res.status(201).send("created successfully");
      }
    );
  } catch (err) {
    res.status(400).send(`error occured ${err}`);
  }
};

export const MarkAsPaid = (_req: Request, res: Response) => {
  const id: any = _req?.params?.id;

  try {
    client.query(
      `UPDATE invoice SET status = $1 where id = $2`,
      ["paid", id],
      (err, result) => {
        if (err) {
          throw new Error(`error ${err?.name}: ${err?.message}`);
        }

        res.status(201).send("mark as paid");
      }
    );
  } catch (err) {
    console.log("error mark as paid ", err);
    res.status(400).send(`err occured ${err}`);
  }
};