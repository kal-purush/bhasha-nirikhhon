import { Request, Response } from "express";
import db from "../database/connection";
import { CustomRequest } from "../middlewares/validar-jwt";
import OpcionTransporte from "../models/opcionTransporte.model";

export const getOpcionTransporte = async (req: Request, res: Response) => {
  try {
    const opcionTransporte = await OpcionTransporte.findAll({
      order: db.col("tipoTransporte"),
    });

    res.json({
      ok: true,
      opcionTransporte,
    });
  } catch (error) {
    res.status(500).json({
      msg: "Hable con el administrador",
      error,
    });
  }
};

export const getUnaOpcionTransporte = async (req: Request, res: Response) => {
  const { id } = req.params;

  try {
    const opcionTransporte = await OpcionTransporte.findByPk(id);

    res.json({
      ok: true,
      opcionTransporte,
    });
  } catch (error) {
    res.status(500).json({
      msg: "Hable con el administrador",
      error,
    });
  }
};

export const crearOpcionTransporte = async (req: Request, res: Response) => {
  const { tipoTransporte } = req.body;

  try {
    const existeOpcionTransporte = await OpcionTransporte.findOne({
      where: { tipoTransporte: tipoTransporte },
    });
    if (existeOpcionTransporte) {
      return res.status(400).json({
        msg: `Ya existe la opción de transporte: ${tipoTransporte}`,
      });
    }

    const opcionTransporte = await OpcionTransporte.build(req.body);

    // Guardar Tipo de documento
    const opcionTransporteCreado = await opcionTransporte.save();

    res.json({
      ok: true,
      msg: `La opción de transporte <b>${tipoTransporte}</b> ha sido creada satisfactoriamente`,
      opcionTransporteCreado,
    });
  } catch (error) {
    res.status(500).json({
      msg: "Hable con el administrador",
      error,
    });
  }
};

export const actualizaropcionTransporte = async (
  req: Request,
  res: Response
) => {
  const { id } = req.params;

  const { body } = req;
  const { tipoTransporte, ...campos } = body;

  try {
    const opcionTransporte = await OpcionTransporte.findByPk(id);
    if (!opcionTransporte) {
      return res.status(404).json({
        msg: `No existe una opción de transporte para el id ${id}`,
      });
    }

    const getNombre = await opcionTransporte.get().tipoTransporte;

    // Actualizaciones
    if (getNombre !== body.tipo) {
      const existeNombre = await OpcionTransporte.findOne({
        where: {
          tipoTransporte: body.tipoTransporte,
        },
      });
      if (existeNombre) {
        return res.status(400).json({
          ok: false,
          msg: `Ya existe una opción de transporte con el nombre ${tipoTransporte}`,
        });
      }
    }

    campos.tipoTransporte = tipoTransporte;

    // Se actualiza el campo
    const opcionTrasnporteActualizado = await opcionTransporte.update(campos, {
      new: true,
    });

    res.json({
      ok: true,
      msg: "Opción de transporte actualizado",
      opcionTrasnporteActualizado,
    });
  } catch (error) {
    res.status(500).json({
      msg: "Hable con el administrador",
      error,
    });
  }
};

export const eliminarOpcionTransporte = async (
  req: CustomRequest,
  res: Response
) => {
  const { id } = req.params;

  try {
    const opcionTransporte = await OpcionTransporte.findByPk(id);
    if (opcionTransporte) {
      const nombre = await opcionTransporte.get().tipoTransporte;

      await opcionTransporte.update({ estado: false });

      res.json({
        ok: true,
        msg: `La opción de transporte ${nombre} se eliminó`,
        id: req.id,
      });
    }

    if (!opcionTransporte) {
      return res.status(404).json({
        msg: `No existe la opción de transporte con el id ${id}`,
      });
    }
  } catch (error) {
    res.status(500).json({
      msg: "Hable con el administrador",
      error,
    });
  }
};

export const activarOpcionTransporte = async (
  req: CustomRequest,
  res: Response
) => {
  const { id } = req.params;
  const { body } = req;

  try {
    const opcionTransporte = await OpcionTransporte.findByPk(id);
    if (!!opcionTransporte) {
      const nombre = await opcionTransporte.get().tipoTransporte;

      if (opcionTransporte.get().estado === false) {
        await opcionTransporte.update({ estado: true });
        res.json({
          ok: true,
          msg: `La opción de transporte ${nombre} se activó`,
          opcionTransporte,
          id: req.id,
        });
      } else {
        return res.status(404).json({
          ok: false,
          msg: `La opción de transporte ${nombre} está activa`,
          opcionTransporte,
        });
      }
    }

    if (!opcionTransporte) {
      return res.status(404).json({
        ok: false,
        msg: `No existe una opción de transporte con el id ${id}`,
      });
    }
  } catch (error) {
    res.status(500).json({
      error,
      msg: "Hable con el administrador",
    });
  }
};