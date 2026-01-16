import '../styles/Section2.css'
import actividadesimg from '../img/actividades.jpeg'
import recorridoimg from '../img/recorrido.jpeg'
import misionimg from '../img/mision.jpeg'
import contactoimg from '../img/contacto.jpeg'
import { BrowserRouter, Link, NavLink, Routes } from 'react-router-dom'
import valijasimg from '../img/Valijas.jpeg'
import entradaAsoc from '../img/entradaAsocFomento.png'
import inauguracionimg from '../img/Inauguracion.jpeg'
import cinta from '../img/Cinta.jpeg'
import in2 from '../img/Inauguracion2.jpeg'
const Section2alt3 = (props) => {
    return (
        <section id="contenedor2">
            <div className='postext'>
                <div className="text">¡SEGUÍ EXPLORANDO NUESTRA PÁGINA!</div>
            </div>
            <Link to="/">
                <article id="parte1">
                    <img src={inauguracionimg} id='actividades' />
                    <div>
                        <h3>VOLVER AL INICIO</h3>
                    </div>
                </article>
            </Link>
            <article id="subcontenedor">
                <Link to="/novedades">
                    <div id="apartado1">
                        <img src={in2} id='muestra' />
                        <div>
                            <h3>NOVEDADES</h3>
                            <p>Aquí podras ver un cronograma de las actividades</p>
                        </div>
                        <div className="overlay"></div>
                    </div>
                </Link>
                <Link to="/mision">
                    <div id="apartado2">
                        <img src={misionimg} id='muestra2' />
                        <div>
                            <h3>NUESTRA MISIÓN</h3>
                            <p>Te contamos un poco sobre que hacemos en el museo</p>
                        </div>
                        <div className="overlay"></div>
                    </div>
                </Link>
                <Link to="/contacto">
                    <div id="apartado3">
                        <img src={entradaAsoc} id='muestra3' />
                        <h3>CONTACTANOS</h3>
                    </div>
                </Link>
            </article>
        </section >

    )
}

export default Section2alt3;