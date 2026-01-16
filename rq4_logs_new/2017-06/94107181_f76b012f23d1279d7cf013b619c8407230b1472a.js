// React - Class: Estado
class Toggle extends React.Component {
  constructor(props) {
    super();
    this.state = { activo: true };
  }
  handleClick(ev) {
    this.setState({
      activo: !this.state.activo
    });
  }
  render() {
    return React.createElement('div', {
      onClick: this.handleClick.bind(this)
    },
      'Activo: ',
      this.state.activo ? 'Si' : 'No'
    );
  }
}
/*
this.setState(nuevoEstado, function() {
  // Aqui this.state ya esta actualizado
});
// Aqui this.state no necesariamente esta
// actualizado
*/
//ReactDOM.render(<Toggle />, document.getElementById("content"));

// React - Estado - Desafio 1: Contador
/*
Contador
Crear un componente Contador, que cada vez que hace click en el botón,
incrementa la variable valor actual en uno.
*/
class Contador extends React.Component {
  constructor(props) {
    super(props);
    this.state = { valor: 1 };
    this.handleClick = this.handleClick.bind(this);
  }

  handleClick() {
    this.setState({ valor: this.state.valor + 1 })
  }

  render() {
    return React.createElement('div', null,
      React.createElement('div', null, this.state.valor),
      React.createElement('button', {
        onClick: this.handleClick
      }, 'Incrementar')
    )
  }
}

//ReactDOM.render(<Contador />, document.getElementById("content"));

// React - Estado - Desafio 2: Fecha Actual
/*
Modifiquemos el componente de forma tal que el render sea puro. Usa la propiedad fechaActual
del estado para mostrar la fecha y hora, y actualiza ese dato cada 1 segundo. Para lograrlo,
No olvides ponerle un valor inicial al estado
Utiliza el setInterval en el constructor para actualizar el estado con el nuevo valor.
Usa la nueva propiedad en el render
Tip
Usa new Date() para obtener el valor de fecha y hora actual.
*/
class FechaActual extends React.Component {
  constructor() {
    super();
    this.state = { fechaActual: new Date() };

    setInterval(function() {
      this.setState({ fechaActual: new Date() });
    }.bind(this), 1000)
  }

  render() {
    return React.createElement('div', null,
      'La fecha actual es: ',
      this.state.fechaActual.toString());
  }
}

//ReactDOM.render(<FechaActual />, document.getElementById("content"));

// React - Estado - Desafio 3: Resumen Listado
// https://codepen.io/jalanya/pen/VbYwNK
/*
https://www.acamica.com/clases/2566/introduccion-a-react/estado/desafio/1874
Desafío 3
Minimiza el estado de este componente y calcula todo lo posible en el render.
Tip
Pista: Presta atención a la información que puedes derivar de otra.
*/
class ResumenListado extends React.Component {
  constructor(props) {
    super(props);

    this.filtrosPosibles = {
      completos: function(item) { return item.completo; },
      borrados: function(item) { return item.borrado; },
    };
    this.state = {
      items: this.props.items,
      filtro: 'completos',
      cantidadFiltro: this.props.items.filter(this.filtrosPosibles.completos).length
    };
  }

  cambiarFiltro(nombreFiltro) {
    return function() {
      var filtro = this.filtrosPosibles[nombreFiltro];
      this.setState({
        filtro: nombreFiltro,
        cantidadFiltro: this.state.items.filter(filtro).length
      });
    }.bind(this);
  }

  render() {
    return React.createElement('div', null,
      'Tienes ',
      this.state.items.length,
      ' elementos y ',
      this.state.cantidadFiltro,
      ' filtrados.',
      React.createElement('button', { onClick: this.cambiarFiltro('completos') }, 'Cantidad completos'),
      React.createElement('button', { onClick: this.cambiarFiltro('borrados') }, 'Cantidad borrados')
    );
  }
}

var itemList = [
  {name: 'A', completo: true, borrado: false},
  {name: 'B', completo: true, borrado: false},
  {name: 'C', completo: true, borrado: false},
  {name: 'D', completo: false, borrado: false},
  {name: 'E', completo: false, borrado: true},
  {name: 'F', completo: false, borrado: true}
]

ReactDOM.render(<ResumenListado items={itemList} />, document.getElementById("content"));

/*
Initial code
class ResumenListado extends React.Component {
  constructor() {
    super();

    this.filtrosPosibles = {
      completos: function(item) { return item.completo; },
      borrados: function(item) { return item.borrado; },
    };
    var filtro = this.filtrosPosibles.completos;
    this.state = {
      filtro: 'completos',
      cantidadFiltro: this.props.items.filter(filtro).length
    };
  }

  cambiarFiltro(nombreFiltro) {
    return function() {
      var filtro = this.filtrosPosibles[nombreFiltro];
      this.setState({
        filtro: nombreFiltro,
        cantidadFiltro: this.props.items.filter(filtro).length
      });
    }.bind(this);

  }

  render() {
    return React.createElement('div', null,
      'Tienes ',
      this.props.items.length,
      ' elementos y ',
      this.state.cantidadFiltro,
      ' filtrados.',
      React.createElement('button', { onClick: this.cambiarFiltro('completos') }, 'Cantidad completos'),
      React.createElement('button', { onClick: this.cambiarFiltro('borrados') }, 'Cantidad borrados')
    );
  }
}
*/