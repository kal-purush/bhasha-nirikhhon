import { Message } from "../../utils/Interfaces";

type Fecha = Date | string | null;
export interface CarpetaEntity {
  IdCarpeta: number;
  Descripcion: string;
  IdCarpetaPadre: number | null| undefined;
  IdUsuario: number;
  Categoria?: "MF" | "FA" | "FS" | undefined;
  Activo?: boolean | null;
  CreadoEl?: Fecha;
  CreadoPor?: string;
  ModificadoEl?: Fecha;
  ModificadoPor?: string;
}
export interface CarpetaCreate {
  IdCarpeta?: number;
  Descripcion: string;
  IdCarpetaPadre: number | null| undefined;
  IdUsuario: number;
  Activo: boolean | null | undefined;
  Categoria: "MF" | "FA" | "FS"| undefined;
  CreadoEl?: Date | string;
  CreadoPor?: string;
}
export interface CarpetaUpdate {
  IdCarpeta?: number;
  Descripcion?: string;
  IdCarpetaPadre?: number;
  IdUsuario?: number;
  Categoria?: "MF" | "FA" | "FS";
  Activo?: boolean | null;
  ModificadoEl?: Date | string;
  ModificadoPor?: string;
}
export interface CarpetaOut {
  message: Message;
  registro?: CarpetaEntity;
}
export interface CarpetasOut {
  message: Message;
  registro?: CarpetaEntity[];
}