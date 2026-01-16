
import React from "react";

const { useState, useEffect } = React;
import styles from './drop-down.css';

import iconExpand from './ic_expand.png';
import iconMinimum from './ic_minimum.png';


export  function Dropdown () {
const data = [
{id: 0, label: "Basic"}, 
{id: 1, label: "Intermediate"},
{id: 2, label: "Hard"},
{id: 3, label: "Expert"}];

  const [isOpen, setOpen] = useState(false);
  const [items, setItem] = useState(data);
  const [selectedItem, setSelectedItem] = useState(null);
  
  function toggleDropdown () {
      
    setOpen(!isOpen);
  }
  
  function handleItemClick (id) {
    selectedItem == id ? setSelectedItem(null) : setSelectedItem(id);
  }
  
  return (

    

    <div className= {styles.dropdown}>
      <div className={styles.dropdownHeader} onClick={toggleDropdown}>
        {selectedItem ? items.find(item => item.id == selectedItem).label : "Chọn độ khó"}
        <i > <img src = {isOpen? iconMinimum: iconExpand}/> </i>
      </div>
      <div className={isOpen? styles.dropdownBody_open:styles.dropdownBody}>
        {items.map(item => (
          <div className={styles.dropdownItem} onClick={e => handleItemClick(e.target.id)} id={item.id}>
            <span className={item.id == selectedItem?styles.dropdownItemDot_selected:styles.dropdownItemDot}>• </span>
            {item.label}
          </div>
        ))}
      </div>
    </div>
  )
}

//export default Dropdown;

// ReactDOM.render(<Dropdown />, document.getElementById('drop-down'));