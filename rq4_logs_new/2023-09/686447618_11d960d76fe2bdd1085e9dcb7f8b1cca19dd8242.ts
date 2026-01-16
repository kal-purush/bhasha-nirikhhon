import { TProducts, TUser } from "./types";

export const users: TUser[] = [{
    id: 'u001',
    name: 'Tati',
    email: 'tati@gmail.com',
    password: 'vavinho123',
    createdAt: new Date().toISOString()
},
{
    id: 'u002',
    name: 'Eric',
    email: 'eric@gmail.com',
    password: 'jadinha123',
    createdAt: new Date().toISOString()
}
];

export const products: TProducts[] = [{
    id: 'p001',
    name: 'Nintendo Switch',
    price: 1339,
    description: 'O melhor portátil que existe',
    imageUrl: 'https://drive.google.com/file/d/10cKfJtuoiej-BNIDBRwIdGgxV_UEVWXV/view?usp=drive_link'
}, 
{
    id: 'p002',
    name: 'PlayStation 4',
    price: 2919,
    description: 'O melhor console que existe',
    imageUrl: 'https://drive.google.com/file/d/10dQN7pDn7Hjw-rEw7jJ2UtNl2CSB8-HW/view?usp=drive_link'

}];