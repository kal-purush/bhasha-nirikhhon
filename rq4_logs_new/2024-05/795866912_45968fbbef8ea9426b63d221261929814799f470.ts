import { AuthContextType } from "@/types/auth.type";
import { createContext } from "react";

// TODO: Add auth context
const AuthContext = createContext<AuthContextType | undefined>(undefined);

export default AuthContext;