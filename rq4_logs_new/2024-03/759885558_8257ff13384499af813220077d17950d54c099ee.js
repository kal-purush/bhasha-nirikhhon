import { getHeroeById } from "./bases/08-import-export";

//Promesas

/* console.log('Antes de la promesa');
const promesa = new Promise( (resolve, reject) => {
    setTimeout( () => {
        // console.log('... 2 segundos después ....');
        //resolve(); 
        const heroe = getHeroeById(2);
        //console.log(heroe);
        resolve(heroe);
    }, 2000 )
});
 
promesa.then( (heroe) => {
    console.log('Heroe: ', heroe);
}) */
 const getHeroeByIdAsync = (id) => {
    return new Promise( (resolve, reject) => {
        setTimeout( () => {
            const resultado = getHeroeById( id );
            if (resultado){
                resolve( resultado )
            }else{reject( 'No se ha encontrado al heroe' )}
            
        }, 2000 )
    })
 }

 getHeroeByIdAsync(9)
    .then( heroe => console.log( 'Heroe: ', heroe ))
    .catch( err => console.warn(err));