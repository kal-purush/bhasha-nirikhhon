import * as fs from "fs";
import { Project } from "ts-morph";

function gen(filename: string) {
  if (!filename.endsWith(".json")) {
    throw Error("filename must end with .json");
  } 
  const content = fs.readFileSync(filename, { encoding: "utf-8" });
  const parsedJson = JSON.parse(content);
  const defaultExportedJson = `export default ${JSON.stringify(
    parsedJson,
    null,
    2
  )} as const;`;

  const project = new Project({
    compilerOptions: {
      declaration: true,
      emitDeclarationOnly: true,
    },
  });
  project.createSourceFile(
    filename.replace(".json", ".ts"),
    defaultExportedJson,
    { overwrite: true }
  );
  project.emitSync();
}

export const run = async () => {
  gen(process.argv[2]);
};