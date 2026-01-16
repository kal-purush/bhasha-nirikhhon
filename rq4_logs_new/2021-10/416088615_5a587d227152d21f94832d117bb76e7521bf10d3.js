import Formulario from "./componentes/Formulario"
import Clima from "./componentes/Clima"
import styled from "@emotion/styled";

const Titulo = styled.h1`
  color: white;
  text-align: center;
  font-size: 50px;
  font-family: Arial, Helvetica, sans-serif;
`

const Contenedor = styled.div`
  margin-top: 60px;
  background-color: white;
  border-radius: 40px;
  height: 50vh;
  display: grid;
  grid-template-columns: 1fr 1fr;
  justify-content: center;
  align-items: center;
  box-shadow: -20px -10px 53px 2px black;
  padding: 20px;
  @media (max-width: 750px) {
    grid-template-columns: 1fr;
    height: auto;
    
      }
`





function App() {
  return (
          <div>
            <Titulo>Clima React App</Titulo>
            <Contenedor>
            
                <Formulario/>

                <Clima/>
   
            </Contenedor>
          </div>
  );
} 

export default App;