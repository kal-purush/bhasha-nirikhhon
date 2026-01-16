const point1 = new Point(3, 4);

console.log(point1);

const segment1 = new Segment(1,1,4,5);

console.log(segment1);
console.log(segment1.length);

const triangle1 = new Triangle(1, 1, 4, 5, 4, 1);

console.log(triangle1);
console.log(triangle1.perimeter);







const client1 = new Client('pippo', 'de pippis', 'via pippolis 27');

console.log(client1.toString());

const employee1 = new Employee('pluto', 'de pippis', 'animali');

console.log(employee1.toString());



// scrivere tutti i dati della classe con to string ad es const s1 = {name: 'manuela', surname: 'ariotti'}
                                                       //         const s2 = {name: 'luca', surname: 'verduci'}



// 1) creare classe studente con toString     
// 2) creare classe classroom che contiene un array di students 
// 3) aggiungere a classroom tre metodi: 
// add student() -----  randomizeStudents() ----- printSquads()  
// per print squad fare un ciclo che prende le coppie dall'array mescolato
// ad esempio : coppia 1  
                // manuela ariotti
                // cesare falzone      
                // coppia 2
                // luca verduci
                // stefano florio                                      