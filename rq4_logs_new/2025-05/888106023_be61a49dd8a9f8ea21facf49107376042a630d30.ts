import { ComponentAppearance } from "@enum/appearances";

const flowAutomaticMessages = {
  errorSendingData: {
    title: "¡Ups! Algo salió mal",
    description: "La solicitud no se pudo radicar correctamente",
    appearance: ComponentAppearance.DANGER,
    duration: 5000,
  },
  errorQueryingData: {
    title: "Error al realizar la acción",
    description:
      "No fue posible realizar la acción, por favor intenta más tarde",
    appearance: ComponentAppearance.DANGER,
    duration: 5000,
  },
  errorCreateRequest: {
    title: "Error en la solicitud",
    description: "Verifique su solicitud en solicitudes en tramite",
    appearance: ComponentAppearance.DANGER,
    duration: 5000,
  },
  SuccessfulCreateRequest: {
    title: "¡la accion solicitud exitosa!",
    description: "Se creo la solicitud con éxito",
    appearance: ComponentAppearance.SUCCESS,
    duration: 5000,
  },
};

export { flowAutomaticMessages };