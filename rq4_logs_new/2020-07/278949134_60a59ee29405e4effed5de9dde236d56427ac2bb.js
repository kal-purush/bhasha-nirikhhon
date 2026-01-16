'use strict';

/**
 *  Victor Diaz Test
 */

const arrgOne = ["Ava", "Emma", "Emma"];
const arrgTwo = ["Olivia", "Sophia", "Emma"];


function removeDuplicates(arrg) {
    const newArrg = [];

    arrg.filter(r => {
        if (newArrg.find(i => i === r)) {
            return true;
          }
          newArrg.push(r);
    })

    return newArrg;
}

function uniqueName(arrgOne, arrgTwo) {

    const cleanArrayOne = removeDuplicates(arrgOne);
    const cleanArrayTwo = removeDuplicates(arrgTwo);

    const matchCoincidence = cleanArrayOne.map(r => {

        // Find coincidence in ArrayTwo
        cleanArrayTwo.filter((f, idx) => {
            if ( f === r) {
                cleanArrayTwo.splice(idx, 1);
            }
        });

        return r;

    });
    return cleanArrayOne.concat(cleanArrayTwo);
}
const arrayResult = uniqueName(arrgOne, arrgTwo);
// ArrayResult should print [ 'Ava', 'Emma', 'Olivia', 'Sophia' ]