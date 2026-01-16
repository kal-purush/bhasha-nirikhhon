import { Response, Request, Router, NextFunction } from "express";
import config from "../lib/config";
const router = Router();
// SDK de Mercado Pago
const mercadopago = require("mercadopago");

mercadopago.configure({
  access_token: config.prod_access_token,
});

router.post("/", (req: Request, res: Response, next: NextFunction) => {
  // Crea un objeto de preferencia
  const { title, unit_price, quantity } = req.body;
  let preference = {
    items: [
      {
        title: title || "bicicleta",
        unit_price: unit_price || 100,
        quantity: quantity || 12,
      },
    ],
  };

  mercadopago.preferences
    .create(preference)
    .then(function (response) {
      res.redirect(response.body.init_point);
    })
    .catch((error) => next(error));
});
export default router;