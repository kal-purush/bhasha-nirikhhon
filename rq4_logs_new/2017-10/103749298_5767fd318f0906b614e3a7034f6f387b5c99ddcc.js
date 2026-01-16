/**
 * Single-variate polynomial
 */
'use strict';
const math = require('mathjs');
const sprintf = require('sprintf');
const utils = require('./utils.js');
const badRingError = new Error("Cannot perform operations for polynomials in different rings");
const noQuoError = new Error("No quotient defined");
const badQuoError = new Error("Quotient with degree < 1 defined");

class Polynomial {
    constructor(arr,quoDeg) {
        this.poly = arr;
        this.deg = getDegree(arr);
        this.quoDeg = quoDeg; // quotients are defined as x^(quoDeg) - 1
    }

    /**
     * Returns the array holding the polynomial array
     */
    get() {
        return this.poly;
    }
    
    /**
     * Returns the textual description of the polynomial with an 
     * optional choice of variable
     * @param {string} variable 
     * @return {string} output
     */
    desc(variable) {
        const self = this;
        let desc = "";
        if (!self.poly) {
            return undefined;
        }
        if (!variable) {
            variable = "x";
        }
        let first = 0;
        self.poly.forEach(function(coeff, index) {
            if (coeff != 0) {
                let sign = (coeff > 0 ? "+" : "-")
                let val = math.abs(coeff);
                if (val == 1) {
                    // Bad types I know...
                    val = "";
                }
                if (index !== first) {
                    desc += sprintf(" %4$s %1$s%3$s^%2$s", val, index, variable, sign);
                } else {
                    if (sign === "-") {
                        desc += sign;
                    }
                    desc += sprintf("%1$s%3$s^%2$s", val, index, variable);
                }
            } else if (index === first) {
                first++;
            }
        });

        return desc;
    }

    equal(polynomial) {
        const self = this;
        let poly1 = self.poly;
        let poly2 = polynomial.poly;
        let deg1 = self.deg;
        let deg2 = polynomial.deg;
        if (deg1 !== deg2 || poly1.length != poly2.length) {
            return false;
        }
        for (let i=0; i<poly1.length; i++) {
            if (poly1[i] !== poly2[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Adds the input polynomial to the current polynomial
     * @param {Polynomial} polynomial 
     * @return {Polynomial} returns the result
     */
    add(polynomial) {
        const self = this;
        if (self.quoDeg !== polynomial.quoDeg) {
            throw badRingError;
        }
        let poly1 = self.poly;
        let poly2 = polynomial.poly;
        let deg1 = self.deg;
        let deg2 = polynomial.deg;
        let newPoly = new Array(Math.max(deg1,deg2)+1).fill(0);
        for (let i=0; i<newPoly.length; i++) {
            let ele1 = poly1[i];
            let ele2 = poly2[i];
            if (!ele1) {
                ele1 = 0;
            }
            if (!ele2) {
                ele2 = 0;
            }
            newPoly[i] += ele1+ele2;
        }
        return new Polynomial(newPoly,self.quoDeg);
    }

    /**
     * Multiplies the input polynomial with the current polynomial
     * @param {Polynomial} polynomial 
     * @return {Polynomial} returns the result
     */
    mult(polynomial) {
        const self = this;
        if (self.quoDeg !== polynomial.quoDeg) {
            throw badRingError;
        }
        let poly1 = self.poly;
        let poly2 = polynomial.poly;
        let deg1 = self.deg;
        let deg2 = polynomial.deg;
        let newPoly = new Array(deg1+deg2+1).fill(0);
        poly1.forEach(function(ele1,ind1) {
            poly2.forEach(function(ele2,ind2) {
                let res = ele1*ele2;
                let newInd = ind1+ind2;
                newPoly[newInd] += res;
            });
        });
        return new Polynomial(newPoly,self.quoDeg);
    }

    /**
     * Computes the representative of the polynomial in the integer ring
     * modulo the input
     * @param {int} modulus
     * @return {Polynomial} returns self
     */
    modBalance(modulus) {
        const self = this;
        let poly = self.poly;
        poly.forEach(function(ele,ind) {
            poly[ind] = utils.balance(ele, modulus);
        });
        self.poly = poly;
        return self;
    }

    /**
     * Computes addition modulo the input
     * @param {Polynomial} polynomial 
     * @param {int} modulus 
     * @return {Polynomial} returns result
     */
    addMod(polynomial, modulus) {
        const self = this;
        let poly = self.add(polynomial);
        return poly.modBalance(modulus);
    }

    /**
     * Computes multiplication modulo the input
     * @param {Polynomial} polynomial 
     * @param {int} modulus 
     * @return {Polynomial} returns result
     */
    multMod(polynomial, modulus) {
        const self = this;
        let poly = self.mult(polynomial);
        return poly.modBalance(modulus);
    }

    /**
     * Computes the polynomial as a representative of the 
     * quotient ring
     */
    applyQuotient() {
        const self = this;
        let quoDeg = self.quoDeg;
        if (!quoDeg) {
            throw noQuoError;
        }
        if (quoDeg < 1) {
            throw badQuoError;
        }
        let poly = self.poly;
        let newPoly = new Array(quoDeg).fill(0);
        for (let i=0; i<poly.length; i++) {
            if (quoDeg > i) {
                newPoly[i] += poly[i];
                continue;
            }
            let newDeg = i;
            while (quoDeg <= newDeg) {
                newDeg = newDeg-quoDeg;
            }
            newPoly[newDeg] += poly[i];
        }
        self.poly = newPoly;
        return self;
    }
}
module.exports = Polynomial;

function getDegree(arr) {
    if (!arr) {
        return 0
    }
    let deg = arr.length-1;
    for (let i=arr.length-1; i>=0; i--) {
        if (arr[i] == 0) {
            deg--;
        } else {
            break;
        }
    }
    return deg;
}