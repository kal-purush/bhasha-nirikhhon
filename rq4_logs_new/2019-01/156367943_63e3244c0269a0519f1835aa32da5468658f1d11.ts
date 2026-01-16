import Referto from "../models/Referto";
import Visita from "../models/Visita";

export default abstract class VisiteController {
  public static addReferto(visita: Visita, referto: Referto) {
    if (visita.referto) { throw new Error("La visita ha gia` un referto"); }
    visita.referto = referto;
  }
}