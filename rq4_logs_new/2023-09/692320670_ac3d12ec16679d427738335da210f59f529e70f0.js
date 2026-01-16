import React from "react";
import Flow from "./Flow";
import styles from './DataProcessing.css';
import LeftMenu from "./LeftMenu";

function DataProcessing() {
    return (
      <div style={{ height: '100%' }}>
        
        <div className="flow-container">
            <LeftMenu/>
             <Flow/>
        </div>
      </div>
    );
  }
  
  export default DataProcessing;