import React, { useState } from 'react'
import { InlineIcon } from '@iconify/react'
import close from '@iconify/icons-mdi/close'
import './ModalAnalisis.css'

const ModalAnalisis = ({ esconder }) => {

  const [ventanas, setVentanas] = useState([])
  const [nuevaVentana, setNuevaVentana] = useState({
    inicio: 0,
    termino: 0,
    etiqueta: ''
  })

  const agregarVentana = e => {
    e.preventDefault()
    setVentanas([
      ...ventanas,
      nuevaVentana
    ])
    setNuevaVentana({
      inicio: 0,
      termino: 0,
      etiqueta: ''
    })
  }

  return (
    <div className="ModalAnalisis" onClick={esconder}>
      <div className="ModalAnalisis__contenedor" onClick={e => e.stopPropagation()}>
        <button className="ModalAnalisis__cerrar" onClick={esconder}>
          <InlineIcon icon={close} />
        </button>
        <form
          className="ModalAnalisis__formulario"
          onSubmit={agregarVentana}
        >
          <h2>Análisis</h2>
          <label>
            Inicio:
            <input
              type="number"
              name="inicio"
              value={nuevaVentana.inicio}
              onChange={e => setNuevaVentana({...nuevaVentana, inicio: e.target.value})}
            /> segundos
          </label>
          <label>
            Fin:
            <input
              type="number"
              name="termino"
              value={nuevaVentana.termino}
              onChange={e => setNuevaVentana({...nuevaVentana, termino: e.target.value})}
            /> segundos
          </label>
          <label>
            Etiqueta:
            <input
              type="text"
              name="etiqueta"
              value={nuevaVentana.etiqueta}
              onChange={e => setNuevaVentana({...nuevaVentana, etiqueta: e.target.value})}
              style={{ width: '7rem' }}
            />
          </label>
          <button type="submit">
            Analizar
          </button>
        </form>
        <div className="ModalAnalisis__contenedor_resultados">
          <h2>Resultados</h2>
          <table className="ModalAnalisis__tabla">
            <thead>
              <tr>
                <th>Etiqueta</th>
                <th>Inicio</th>
                <th>Fin</th>
                <th>Duración</th>
              </tr>
            </thead>
            <tbody>
              {ventanas.map((v, i) => (
                <tr key={`fila-ventana-${i}`}>
                  <td>{v.etiqueta}</td>
                  <td>{v.inicio}</td>
                  <td>{v.termino}</td>
                  <td>{v.termino - v.inicio}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}

export default ModalAnalisis