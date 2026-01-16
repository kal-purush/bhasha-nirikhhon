import React, {createContext, useState} from 'react'
export const AppContext=createContext();

export const DataProvider=({children})=>{

  const [usuario, setUsuario] = useState({});
  const [paciente, setPaciente] = useState({});
  const [usuarios, setUsuarios] = useState([]);
  
  const [solicitudes, setSolicitudes] = useState([])
  const [url, setUrl] = useState('')
  const [solicitud, setSolicitud] = useState({})
  const [recordatorios, setRecordatorios] = useState([])

  return (
    <AppContext.Provider value={{
      usuario,setUsuario,
      solicitudes,setSolicitudes,
      solicitud, setSolicitud,
      url, setUrl,
      usuarios, setUsuarios,
      paciente, setPaciente,
      recordatorios, setRecordatorios
      
    }}>{children}</AppContext.Provider>
  )
}