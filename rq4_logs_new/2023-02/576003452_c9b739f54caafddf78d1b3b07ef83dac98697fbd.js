// ! 1. Sinhrono programiranje

// console.log(`Good morning!`);

// const myDailyRoutine = () => {
//     console.log(`Work`);
//     console.log(`Gym`);
//     console.log(`Sleep`);
// }

// myDailyRoutine();

// console.log(`Good night!`);


// ! 2. Asinhrono programiranje

console.log(`Good morning`);

setTimeout(() => {
    console.log(`Hello`);
}, 2000);

// setInterval(); // * jos jedna asinhrona funkcija

console.log(`Good night!`);


// ! Nije svaka funkcija koja koristi neku call-back funkciju asinhrona!

// * primer: map, forEach, filter, reduce, itd...

console.log(`Started counting`);

const testNums = [1,2,3,4,5];

testNums.forEach(num => console.log(num));

console.log(`Stopped counting`);



// Primer:

console.log(`Started logging a user in`);

// zamislite da smo poslali zahtev nekom serveru umesto ovog setTimeout-a
const loginUser = (email, password) => {
    setTimeout(() => {
        return {
            email,
            password,
            isLoggedIn: true
        }
    }, 1000);
}

const user = loginUser('test@gmail.com', 'test123');

console.log(user); // ! undefined


// * 1. nacin --> CALL BACK funkcija

// * 2. nacin --> Promise je objekat koji ili vraca rezultat uspesno izvrsene asinhrone operacije ili rezultat neuspesne operacije.

// Promise moze biti u 3 stanja:
// 1. PENDING (stanje cekanja)
// 2. RESOLVED (uspesno razresen)
// 3. REJECTED (odbaceno stanje)

const promise = new Promise((resolve, reject) => {
    setTimeout(() => {
        resolve({
            username: 'test.testic',
            fullname: 'Test Testic'
        });
        reject(new Error(`This user is not logged in!`));
    }, 4000);
});

promise
    .then(data => console.log(data)) // ! then block hvata vrednost (sabskrajbuje se na vrednost) uspesno razresenog promise-a (rezultata asinhrone operacije)
    .catch(e => console.log(e)) // ! catch block hvata gresku koju vraca kao svoj rezultat asinhrona operacija
    // .finally  // ! finally metoda se poziva bez obzira da li je promise razresen uspesno ili neuspesno (dakle u oba slucaja)


// Realni primer:

const adminLoginAirbnb = (email, password) => {
    return new Promise((resolve, reject) => {
        setTimeout(() => {
            if (password === 'superadmin') {
                console.log(`Hello Admin, you have logged in with email ${email}`);
                resolve({
                    email,
                    password
                });
                return;
            }
            reject(new Error(`Login credentials are wrong. Please try again!`));
        }, 5000);
    });
}

const getRecensionsForUser = (email) => {
    return new Promise((resolve, reject) => {
        setTimeout(() => {
            resolve([5,9,8,10,2,4]);
            reject(new Error(`Failed while getting recensions for user with email: ${email}`));
        }, 500);
    });
}


adminLoginAirbnb('admin@gmail.com', 'superadmin')
    .then(user => {
        const email = user.email;
        // console.log(user);
        getRecensionsForUser(email)
            .then(recensions => {
                console.log(`Recensions for ${email} are:`, recensions);
            })
            .catch(e => console.log(e.message));
    })
    .catch(e => console.log(e.message));

// ! Ovako moze nastati PROMISE HELL -> kada previse ugnjezdenih THEN poziva postoji



// * 3. ASYNC - AWAIT

// * Ove dve reci su rezervisane reci i koriste se u pisanju asinhronog koda na "sinhroni nacin"


const getItem = (itemID) => {
    return new Promise((resolve, reject) => {
        setTimeout(() => {
            resolve({
                itemName: `Samsung Galaxy S22`,
                color: 'black',
                memory: '128gb'
            });
            reject(new Error(`Error while fetching the data about item ${itemID}`));
        }, 10000);
    })
}

const getItemDescription = (item) => {
    return new Promise((resolve, reject) => {
        setTimeout(() => {
            resolve({
                shortDescription: 'Smart Phone',
                longDescription: `Smart Phone made by Samsung company from South Korea`
            });
            reject(new Error(`Error while fetching item description`));
        }, 1000);
    })
}

async function showItem() {
    try {
        const item = await getItem('dsaflj12312');
        const itemDesc = await getItemDescription(item);
        console.log(item);
        console.log(itemDesc);
    } catch (error) {
        console.log(error);
    }
}

showItem();






