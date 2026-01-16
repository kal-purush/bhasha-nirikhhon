// generateComponent.js
const fs = require("fs");
const path = require("path");

const TEMPLATES_FOLDER = "generator_templates";

const generateComponent = (name, type) => {
  const nameFirstUpper = name.charAt(0).toUpperCase() + name.slice(1);
  const rootFolderPath = path.join(__dirname, "..");

  const controllerTemplate = path.join(
    rootFolderPath,
    TEMPLATES_FOLDER,
    "ViewModelTemplate.txt"
  );
  const viewTemplate = path.join(
    rootFolderPath,
    TEMPLATES_FOLDER,
    "ViewTemplate.txt"
  );
  const styleTemplate = path.join(
    rootFolderPath,
    TEMPLATES_FOLDER,
    "StyleTemplate.txt"
  );
  const modelTemplate = path.join(
    rootFolderPath,
    TEMPLATES_FOLDER,
    "ModelTemplate.txt"
  );

  const targetDirectory =
    type === "screens"
      ? path.join(rootFolderPath, "app", "screens", name.toLowerCase())
      : path.join(rootFolderPath, "app", "components", name.toLowerCase());

  // Create the target directory if it doesn't exist
  if (!fs.existsSync(targetDirectory)) {
    fs.mkdirSync(targetDirectory, { recursive: true });
  }

  const controllerContent = fs
    .readFileSync(controllerTemplate, "utf8")
    .replace(/\${name}/g, nameFirstUpper);
  fs.writeFileSync(
    path.join(targetDirectory, `${nameFirstUpper}ViewModel.js`),
    controllerContent
  );

  const viewContent = fs
    .readFileSync(viewTemplate, "utf8")
    .replace(/\${name}/g, nameFirstUpper);
  fs.writeFileSync(
    path.join(targetDirectory, `${nameFirstUpper}View.js`),
    viewContent
  );

  const styleContent = fs
    .readFileSync(styleTemplate, "utf8")
    .replace(/\${name}/g, nameFirstUpper);
  fs.writeFileSync(
    path.join(targetDirectory, `${nameFirstUpper}Style.js`),
    styleContent
  );

  const modelContent = fs
    .readFileSync(modelTemplate, "utf8")
    .replace(/\${name}/g, nameFirstUpper);
  fs.writeFileSync(
    path.join(targetDirectory, `${nameFirstUpper}Model.js`),
    modelContent
  );

  console.log(
    `Component '${nameFirstUpper}' generated successfully in ${targetDirectory} directory!`
  );
};

const name = process.argv[2];
const type = process.argv[3] || "component";

if (!name) {
  console.error("Please provide a component name.");
  process.exit(1);
}

generateComponent(type, name);