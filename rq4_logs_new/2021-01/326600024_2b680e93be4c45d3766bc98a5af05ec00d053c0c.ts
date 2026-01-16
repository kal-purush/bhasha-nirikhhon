

import {
    IsLastInStock,
    averageDollarValue,
    fastestCar,
    getEachAccountsTotalPayout,
} from './index'

import chai from 'chai'
const expect = chai.expect

const cars = [
    {
        name: 'Aston Martin One-77',
        horsepower: 750,
        dollarValue: 1850000,
        inStock: true,
    },
    {
        name: 'Nissan Skyline GTR',
        horsepower: 850,
        dollarValue: 1950000,
        inStock: true,
    },
];

describe('is last in stock', () => {
    it("should run the test", () => {
        expect(IsLastInStock(cars)).to.be.true
    })
})

describe('average dollar value', () => {
    it("should run the test", () => {
        expect(averageDollarValue(cars)).to.be.true
    })
})

describe('fastest car', () => {
    it("should run the test", () => {
        expect(fastestCar(cars)).to.be.true
    })
})

describe('fastest car', () => {
    it("should run the test", () => {
        expect(getEachAccountsTotalPayout(cars)).to.be.true
    })
})
