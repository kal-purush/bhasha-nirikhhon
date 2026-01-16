import { Interface } from "readline";

export interface User {
  id: number;
  email: string;
  avatarPath: string;
  displayName: string;
  isAdmin: any;
  status: boolean;
}