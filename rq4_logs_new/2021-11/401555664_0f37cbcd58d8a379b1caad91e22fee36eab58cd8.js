import { useState, useEffect } from "react";
import { useParams } from "react-router-dom";
import Category from "../../Components/Category/Category";
import {getFirestore} from "../../firebase/index";


const CategoryContainer = () => {
    const [products, setProducts] = useState([]);
    const {id} = useParams();


    useEffect(() => {
       const db = getFirestore();
       const itemCollection = db.collection("productos").where("category", "==", id);
       
       itemCollection.get().then(response => {
           if(response.size === 0 ) {
               console.log("no hay productos");
               alert("No hay productos");
           } else {
               setProducts(response.docs.map((doc) => {
                return { id: doc.id, ...doc.data() };
               })) 
           }

       }) 
    })

    return (
        <div>
            <Category products={products} />
        </div>
    )
}

export default CategoryContainer