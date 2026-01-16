import { User } from "@/models/User";

export const fetchUser = (): Promise<{ data: User[] }> =>
  fetch("api/user").then((res) => res.json());