import { Request, Response } from "express";

// Controlador para calcular el IMC y la TMB
export const calcularIMCyTMB = (req: Request, res: Response): void => {
  const { genero, peso, talla, edad } = req.body;

  // Validación básica
  if (!genero || !peso || !talla || !edad) {
    res.status(400).json({ error: "Todos los campos son requeridos" });
    return;
  }

  // Cálculo del IMC
  const imc = peso / (talla ** 2);

  // Cálculo de la TMB
  let tmb;
  if (genero === "Masculino") {
    tmb = 10 * peso + 6.25 * (talla * 100) - 5 * edad + 5;
  } else if (genero === "Femenino") {
    tmb = 10 * peso + 6.25 * (talla * 100) - 5 * edad - 161;
  } else {
    res.status(400).json({ error: "Género no válido" });
    return;
  }

  // Respuesta con los resultados
  res.json({
    imc: imc.toFixed(2),
    tmb: tmb.toFixed(2),
  });
};