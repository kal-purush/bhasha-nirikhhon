import fs from "node:fs/promises";

const timestamp = Math.floor(Date.now() / 1000) - 86400;
const insertStatements = [...Array(288).keys()]
  .map((i) => [timestamp + i * 5 * 60, 10 + i * 0.03472, 20 - i * 0.03472])
  .map(
    ([timestamp, temperature, humidity]) =>
      `INSERT INTO measurements VALUES(${timestamp},${temperature},${humidity});`
  )
  .join("\n");

const command = `DELETE FROM measurements;
${insertStatements}`;

await fs.writeFile("testdata.sql", command);