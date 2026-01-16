import timer from "./timer.js"; //export != export default (pas de {timer}) import timer , {p1, p2}
import io from "./io.js"; 
export function exp01() {
    timer.pause(5000, () =>{
        console.log('5 secondes se sont écoulées !');
    })
}
//calback imbriquee 


export function exp02(){
    timer.Promises.pause(5000)
    .then(response => { // pas time mais reponse d objet {time, message}
        console.log(`pause de ${response.time / 1000} secondes : ${response.message}`);
        return response.message;
    })
    .then(message => {
        console.log(`message recu de premier then : ` + message);
        return true;
    })
    .then(status => console.log(status));

}

export async function exp03(){ // syntaxe async/await
    try{
        let response = await timer.Promises.pause(5000); // resolve(response)
        console.log(`${response.message} apres ${response.time / 1000} secondes`);
        return true ;
    } catch(err){ // reject(err)
        console.log('erreur : ' + err.message);
        return false;
    }
} 


export function exp04(){
    io.readFile('data/test.txt', data => {
        console.log('---- donnees lues : ------');
        console.log(data);
        console.log('--- Fin fichier -----');
    })
}

export function exp05(){
    io.promises.readFile('data/tesst.txt')
    .then(data => {
        console.log('---- donnees lues : ------');
        console.log(data);
        console.log('--- Fin fichier -----');
    })
    .catch(err => {
        console.log('erreur de lecture : ' + err.message);
    })
}

// fct return promiss et daans la fct de resolution on a param boolean


// => await async wait : blockage async  , apres await on a tjrs promese
// garantie que tu lance le traitement o matsnahach itssala
//synchrone => fct bloquante
//async elle meme promisse
// fct ordinaire se sont des fcts pas asynchrone 


// executeur comme fct callback : executeur resolve reject : sont 3fct callback :
// traitement plutiot sequentiel 
// je suis sur avec then que le deuxieme ne s execute que le 1er est execute => async

/* 
then c est aussi une promise elle meme poura realiser trait asyn ou retour appele autre fct async appel fct et return ce que va retourn 
traitement declaratique de meme niv pas hierchique then return qlq chose et en le recupere dans .then
*/



// le reject : une seul sol : try catch