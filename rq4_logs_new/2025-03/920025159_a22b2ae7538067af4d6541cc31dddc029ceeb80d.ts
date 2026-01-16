import { initDB } from "./src/db/conn"

beforeAll(async () => {
    initDB();
})