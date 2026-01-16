import { useState } from "react";

export default function CategoriaForm() {

    const [pregunta, setPregunta] = useState('¿De qué color es el corazón?');
    const [opcion1, setOpcion1] = useState('Azul');
    const [opcion2, setOpcion2] = useState('Rojo');
    const [opcion3, setOpcion3] = useState('Verde');
    const [seleccion, setRespuesta] = useState(null);

    const procesarFormulario = async (eventoSubmit) => {
        try {
          eventoSubmit.preventDefault();
          if (!opcion1 || !opcion2 || !opcion3 || opcion1.trim() === '' || opcion2.trim() === '' || opcion3.trim() === '') {
            throw new Error('Debe proporcionar opciones válidas.');
          }
      
          const preguntaData = {
            pregunta,
            opcion: seleccion,
          };
          
          const baseUrl   = 'http://localhost:3000';
          const url       = baseUrl + '/categoria';
          const respuesta = await fetch(url, {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json'
            },
            body: JSON.stringify( preguntaData )
          });
          
          if (!respuesta.ok) throw new Error("No se pudo guardar la categoría...");
          
          const categoriaGuardada = await respuesta.json();
          console.dir( categoriaGuardada );
        } catch (error) {
          console.error( error );
        }
      };
      
    

return (
    <>
        <h1 className="titulo">{pregunta}</h1>
        <form onSubmit={procesarFormulario}>
        <div>
            <input type="radio" id="opcion1" name="respuesta" value={opcion1} onChange={(e) => setRespuesta(e.target.value)}/>
            <label htmlFor="opcion1">{opcion1}</label>
        </div>
        <div>
            <input type="radio" id="opcion2" name="respuesta" value={opcion2} onChange={(e) => setRespuesta(e.target.value)}/>
            <label htmlFor="opcion2">{opcion2}</label>
        </div>
        <div>
            <input type="radio" id="opcion3" name="respuesta" value={opcion3} onChange={(e) => setRespuesta(e.target.value)}/>
            <label htmlFor="opcion3">{opcion3}</label>
        </div>
        <button type="submit">Responder</button>
        </form>
        {seleccion && (
        <p>Tu respuesta: {seleccion}</p>
        )}
    </>
);
}