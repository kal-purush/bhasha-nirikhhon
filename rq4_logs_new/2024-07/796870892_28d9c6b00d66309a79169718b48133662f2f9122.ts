import { StoreContext, StoreContextProps } from "@/utils/store";
import { useContext } from "react";

export const useRoles = () => {
  const { authUser } = useContext(StoreContext) as StoreContextProps;
  const isAdmin = authUser?.role === "admin";
  const isManager = authUser?.role === "manager";
  const isSuperAdmin = authUser?.role === "super-admin";
  const isSalesPersonnel = authUser?.role === "sales-personnel";

  return {
    isAdmin,
    isManager,
    isSuperAdmin,
    isSalesPersonnel
  };
};