import Folder from "./components/Folder";
import "./styles.css";
import { FolderData } from "./data/FolderData";
import { useState } from "react";
import useTraverseTree from "./hooks/traverseTree";

export default function App() {

  const [allData, setAllData]= useState(FolderData)
  const {insertNode}= useTraverseTree()
   
  const handleNewNode=(folderId, item,isFolder)=>{
   const newTree=insertNode(allData, folderId, item,isFolder)
   setAllData(newTree)
  }
  return (
    <div className="App">
      <Folder handleNewNode={handleNewNode} allData={allData}/>
    </div>
  );
}