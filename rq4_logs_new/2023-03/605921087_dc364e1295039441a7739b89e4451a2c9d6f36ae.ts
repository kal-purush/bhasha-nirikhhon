import { useEffect, useState } from "react";
import { supabase } from "../../supabaseClient";

export default function DataBase() {
    const [datos, setDatos] = useState([]);
    
    useEffect(() => {
        async function fetchDatos() {
          let { data: string, error } = await supabase.from("nombre_de_la_tabla").select("*");
          if (error) console.log("Error al recuperar datos de Supabase:", error);
          else setDatos(datos);
        }
        fetchDatos();
    }, []);
    
    return (
        <>
        </>
  )
    
}