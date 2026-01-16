import User from "@project1-chat-app/shared";
import { saveNewUser } from "./db"

export const saveUser = async (newUser: User): Promise<void> => {

  await saveNewUser(newUser);

};

export default saveUser;