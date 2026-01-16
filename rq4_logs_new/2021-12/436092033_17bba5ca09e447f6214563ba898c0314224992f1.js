import { useState } from "react";



function AddItem(props){
const [name, setName] = useState("");
const [price, setPrice] = useState(0);
const [type, setType] = useState("");
const [brand, setBrand] = useState("");


const addItemButtonPressed=() =>{
    props.addItem({
        name: name, 
        price : price, 
        type : type, 
        brand: brand
    });
 setName("");
 setPrice(0);
 setType("");
 setBrand("");
};
    return(
        <div className="container">
            <div className="row mt"> <h2>Add a Item</h2></div>
            <div className="row">
            <label htmlFor="name-field">Name:</label>
               <input id="name-field" className="form-control" type="text" value ={name} 
               // parameter e is passed from the onChange event handler and call setName, updating the name state
               onChange={(e) => setName(e.target.value)} />

               <label htmlFor="price-field">Price:</label>
               <input id="price-field" className="form-control" type="number" value ={price}  
               onChange={(e) => setPrice(e.target.value)} />
               
               <label htmlFor="type-field">Type:</label>
               <input id="type-field" className="form-control" type="text" value ={type} 
               onChange={(e) => setType(e.target.value)} />

               <label htmlFor="brand-field">Brand:</label>
               <input id="brand-field" className="form-control" type="text" value ={brand} 
               onChange={(e) => setBrand(e.target.value)} />
            </div>
              
            <div className="row mt-4">
               <button type = "button" className="btn btn-primary" onClick={addItemButtonPressed}>Add Item</button>
            </div>                  
        </div>
    )
}

 export default AddItem;