import React from "react";
import { Link } from "react-router-dom";

const Banda = (props) => {
    return(
        

        <tr className="bandas">
            <td className= "line">{props.banda.id}</td>
            <td>{props.banda.name}</td>
            <td>{props.banda.country}</td>
            <td>{props.banda.genre}</td>
            <td>{props.banda.foundation_year}</td>
      
        </tr>

        

        
    );

};
export default Banda;