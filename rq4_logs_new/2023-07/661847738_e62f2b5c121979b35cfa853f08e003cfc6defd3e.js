import React from "react";
import "../../style/Scrapeo.css";

const Scrapeo = () => {
  return (
    <>
      <div className="proyectos-container">
        <h1 className="title">
          Escrapeo de datos de páginas web: Obtención de productos
        </h1>
        <div className="proyecto-details">
          <h3 className="">Descripción:</h3>
          <p>
            En este Script, exploraremos el proceso de escrapeo de datos de la
            página web de Acadesa, centrándonos en la obtención de más de 1000
            productos y la extracción de información.
          </p>
          <p>
            Al finalizar el proceso de ejecución del script de escrapeo, se
            generará un archivo Excel que contendrá todos los productos
            scrapeados
          </p>
          <p>
            El proceso de escrapeo de datos puede aplicarse a diversas páginas
            web siempre y cuando se estudie la estructura HTML de cada página en
            particular. Cada sitio web puede tener su propia estructura y
            etiquetas HTML específicas para los datos que se desean extraer.
          </p>
          <p>
            Es posible obtener datos sobre ofertas, novedades, productos más
            vendidos y otros aspectos relevantes para un análisis más completo y
            una toma de decisiones informada.
          </p>
          <h3 className="">Tecnologías usadas:</h3>
          <ul>
            <li>Python:</li>
            <p>
              instalando librerias playwright para automatización,numpy manejos
              de array para datos, pandas para leer y crear archivos CSV,Excel
            </p>
          </ul>
          <h3 className="">Código de la App</h3>
          <p>
            <a
              href="https://github.com/OCHANDO97/webScraping"
              target="_blank"
              rel="noopener noreferrer"
            >
              Ir a GitHub
            </a>
          </p>
        </div>
      </div>
    </>
  );
};

export default Scrapeo;