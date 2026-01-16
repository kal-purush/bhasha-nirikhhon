import { Application } from "https://deno.land/x/oak/mod.ts";
import handler, { UserHandler } from "./user/userHandler.ts";
import UserRepository from "./user/userRepository.ts";
import { errorHandle } from "./utils/middleware.ts";
import { createPool } from "./db.ts";

const db = await createPool();
const userRepo = new UserRepository(db);
const userHandler = new UserHandler(userRepo);

const app = new Application();

app.use(errorHandle);
handler(app, userHandler);

await app.listen({ port: 8000 });