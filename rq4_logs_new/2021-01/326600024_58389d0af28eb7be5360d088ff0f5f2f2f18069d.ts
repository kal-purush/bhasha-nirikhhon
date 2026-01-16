import R from 'ramda'

type Car = {
    name: string;
    horsepower: number;
    dollarValue: number;
    inStock: boolean;
}

const car = {
    name: 'Aston Martin One-77',
    horsepower: 750,
    dollarValue: 1850000,
    inStock: true,
}

const car2 = {
    name: 'Aston Martin Two-77',
    horsepower: 850,
    dollarValue: 1950000,
    inStock: true,
}


const IsLastInStock = R.compose<Car[], Car, boolean>(R.prop('inStock'), R.last)

const averageDollarValue = R.compose<Car[], Number[], Number>(R.converge(R.divide, [R.sum, R.length]), R.map(R.prop('dollarValue')))

const fastestCar = R.compose<Car[], Car[], Car, String, String>(R.flip(R.concat)(' is the fastest'), R.prop('name'), R.last, R.sortBy(R.prop('horsepower')))

export {
    IsLastInStock,
    averageDollarValue,
    fastestCar
}









