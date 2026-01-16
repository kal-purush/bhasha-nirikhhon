import { endpointUser } from "@controller/routes";

import {
  getAllSwager
} from "./getAll";
import { getByTokenSwagger } from "./getByToken";
import { getConversationsFromUserSwagger } from "./getConversationsFromUser";
import { GetByIdSwagger } from "./getUserById";
import { loginSwagger } from "./login";
import { registerSwagger } from "./register";
const userSwagger = {
  [`/${endpointUser}/getAll`]: {
    get: { ...getAllSwager }
  },
  [`/${endpointUser}/conversations`]: {
    get: { ...getConversationsFromUserSwagger }
  },
  [`/${endpointUser}/`]: {
    get: { ...getByTokenSwagger }
  },
  [`/${endpointUser}/{id}`]: {
    get: { ...GetByIdSwagger }
  },
  [`/${endpointUser}/login`]: {
    post: { ...loginSwagger }
  },
  [`/${endpointUser}/register`]: {
    post: { ...registerSwagger }
  }

};
export { userSwagger };