import { getMeI } from "@franchise/services/auth/useGetMe";
import { QueryKeys } from "@franchise/services/queryKeys";
import { z } from "zod";
import { queryClient } from "../../services/queryClient";

const permissionType = z.enum([
  "FRANQUIA_CADASTRO",
  "FRANQUIA_ACORDO",
  "PROMOTOR_CADASTRO",
  "PROMOTOR_ACORDO",
  "CLIENTE_CADASTRO",
  "CLIENTE_ACORDO",
  "EVENTOS_CADASTRO",
  "EVENTOS_RELATORIO",
  "RELATORIOS",
  "TERMINAIS_GERENCIAMENTO",
  "TERMINAIS_ACESSO",
  "TERMINAIS_MODELOS",
  "TERMINAIS_ADQUIRENTE",
  "TERMINAIS_PENDENTES",
  "TERMINAIS_RASTREIO",
  "FICHAS_CARDARPIO",
  "FICHAS_USUARIOS",
  "FICHAS_ESTOQUE",
  "FICHAS_LOCAL_ENTREGA",
  "DASHBOARD",
  "TERMINAIS_INCLUSAO",
  "TERMINAIS_HISTORICO",
  "TERMINAIS_LICENCAS",
  "LOGIN_BACKOFFICE",
  "CANCELAMENTO",
  "NOTIFICACOES",
  "MEUS_DADOS",
]);
export type PermissionEnum = z.infer<typeof permissionType>;

const functionEnum = z.enum(["create", "view", "delete"]);

export type FunctionEnum = z.infer<typeof functionEnum>;

export const getPermission = (
  permission: PermissionEnum,
  action: FunctionEnum
) => {
  const user = queryClient.getQueryData(QueryKeys.GET_ME) as getMeI;

  switch (action) {
    case "create":
      return user?.UserPermission?.find(
        (userPermission) => userPermission?.Feature?.key === permission
      )?.create;
    case "view":
      return user?.UserPermission?.find(
        (userPermission) => userPermission?.Feature?.key === permission
      )?.view;

    case "delete":
      return user?.UserPermission?.find(
        (userPermission) => userPermission?.Feature?.key === permission
      )?.delete;
    default:
      return false;
  }
};