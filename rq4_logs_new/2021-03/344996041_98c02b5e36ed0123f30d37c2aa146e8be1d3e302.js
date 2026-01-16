import './DropdownHeader.css';

import React from 'react';
import { FiChevronUp } from "react-icons/fi";
import { FiChevronDown } from "react-icons/fi";

function DropdownHeader(props) {
    return (
        <div className="card-header ddHeader" onClick={props.onToggle}>
            <span id="headerTitle">{props.headerText()}</span>
            <span id="headerArrow">{props.listOpen ? <FiChevronUp/> : <FiChevronDown/>}</span>
        </div>
    );
}

export default DropdownHeader;