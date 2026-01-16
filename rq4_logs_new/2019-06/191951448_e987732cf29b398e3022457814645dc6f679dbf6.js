class Menu {
    // método que se ejecuta al crear una nueva instancia de Menú
    constructor() {
        this.enlaces     = [];
        this.descripcion = [];
    }

    // Se recibe el enlace y descripción del enlace que deseamos agregar al menú
    insertar(enlace, descripcion) {
        // Se agrega el enlace recibido en la lista de enlaces
        this.enlaces.push(enlace);
        // Se agrega la descripción recibida en la lista de descripciones
        this.descripcion.push(descripcion);
    }

    // Se recibe el Id del elemento donde se desea cargar el menú generado
    mostrar(elemento) {
        // Se declara el contador
        let x;
        // Se declara la cadena donde se irá armando el HTML del menú
        let cadena = '<div>';
        
        // Por cada enlace
        for(x=0; x < this.enlaces.length; x++)
            // Se utilizan las comillas invertidas para utilizar el valor del elemento con ${}
            cadena += `<a href="${this.enlaces[x]}">${this.descripcion[x]}</a><br>`;
        
        cadena += '</div>';

        // Se inserta el menú armado en el elemento HTML definido por su Identificador
        document.getElementById(elemento).innerHTML = cadena;
    }
}