import React from 'react'
import Encabezado from './encabezado';
import Tablero from './tablero';
import './app.css';
import { library } from '@fortawesome/fontawesome-svg-core'
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome'
import { faStroopwafel, faCartArrowDown } from '@fortawesome/free-solid-svg-icons'
import constructor from './util/constructor'

library.add(faStroopwafel)

const initState = () =>{
    const baraja = constructor();
    return{
        baraja,
        parejaSeleccionada: [],
        estaComparando: false
    };
}
export default class App extends React.Component {
    
    constructor(props){
        super(props);
        this.state = initState();
        this.state.baraja.map((tarjeta) => {
        })
    }

    render(){
        return (
            <div className="App">
                <Encabezado />
                <Tablero
                    baraja={this.state.baraja}
                    parejaSeleccionada={this.state.parejaSeleccionada}
                    seleccionarTarjeta={(tarjeta) => this.seleccionarTarjeta(tarjeta)}
                />
            </div>
        )        
    }

    seleccionarTarjeta(tarjeta){
        if (
            this.state.estaComparando ||
            this.state.parejaSeleccionada.indexOf(tarjeta) > -1 ||
            tarjeta.adivinada
        ){
            return;
        }
        const parejaSeleccionada = [...this.state.parejaSeleccionada, tarjeta];
        this.setState({
            parejaSeleccionada
        });
        if (parejaSeleccionada.length ===2){
            this.compararPareja(parejaSeleccionada);
        }
    }

    compararPareja(parejaSeleccionada){
        this.setState({estaComparando: true});
        setTimeout(() =>{
            const [primeraTarjeta, segundaTarjeta] = parejaSeleccionada;
            let baraja = this.state.baraja;
            if(primeraTarjeta.icon === segundaTarjeta.icon){
                baraja = baraja.map((tarjeta) =>{
                    if(tarjeta.icon !== primeraTarjeta.icon){
                        return tarjeta;
                    }
                    return{...tarjeta, adivinada: true};
                });
            }
            this.setState({
                parejaSeleccionada: [],
                baraja,
                estaComparando: false
            })
        }, 1000)
    }

}