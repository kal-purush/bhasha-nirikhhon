import React from "react";
import FileTree from "./FileTree";


const root = {
  name: "My workspace",
  type: "dir",
  children: [
    {
      name: "Design projects",
      type: "dir",
      children: [
        {
          name: "App Redesign",
          type: "file",
        },
      ],
    },
    {
      name: "Development",
      type: "dir",
      children: [
        {
          name: "Frontend Tasks",
          type: "file",
        },
      ],
    },
    {
      name: "Marketing",
      type: "file",
    },
    {
      name: "Sales pitch",
      type: "file",
    },
  ],
};

const App = props => (
  <div id="app">
    <FileTree data={root}/>
  </div>
);

export default App;