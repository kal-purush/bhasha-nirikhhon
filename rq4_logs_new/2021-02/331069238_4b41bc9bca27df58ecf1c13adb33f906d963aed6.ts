import axios from 'axios';
const URL: string = 'http://localhost:3001';

export const uploadAction = async (file: any) => {
  //el HTML requerido para generar la funcionalidad de Multer es incompatible con la sintaxis jsx
  //por eso se genera un Form por una funcionan, se agrega una propiedad con value y por header le asignamos el tipo
  // de upload que va a ejecutar, en este caso multiples archivos

  const fd = new FormData();
  fd.append('file', file);
  const config = {
    headers: {
      'Content-Type': 'multipart/form-data'
    }
  }


  try {
    await axios.post(`${URL}/multer/subirArchivo`, fd, config);
  } catch (err) {
    console.log(err);
  }
}
