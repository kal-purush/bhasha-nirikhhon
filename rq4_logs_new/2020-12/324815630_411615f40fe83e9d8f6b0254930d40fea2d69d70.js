import React from "react";
import { ResourcesContextProvider } from "scripture-resources-rcl";
import Scripture from "./components/Scripture";

function App() {
  const config = { server: "https://git.door43.org" };

  const resourceLinks = [
    "unfoldingWord/el-x-koine/ugnt/master",
    "unfoldingWord/en/ult/v5",
    "unfoldingWord/en/ust/v5",
    "ru_gl/ru/rlob/master"
  ];

  const reference = { bookId: "3jn" };

  const [resources, setResources] = React.useState([]);
  return (
    <ResourcesContextProvider
      resourceLinks={resourceLinks}
      resources={resources}
      onResources={setResources}
      config={config}
      reference={reference}
    >
      <Scripture />
    </ResourcesContextProvider>
  );
}

export default App;