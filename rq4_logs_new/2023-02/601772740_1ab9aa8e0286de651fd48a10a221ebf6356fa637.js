// Returns a random DNA base
const returnRandBase = () => {
  const dnaBases = ['A', 'T', 'C', 'G'];
  return dnaBases[Math.floor(Math.random() * 4)];
}

// Returns a random single strand of DNA containing 15 bases
const mockUpStrand = () => {
  const newStrand = [];
  for (let i = 0; i < 15; i++) {
    newStrand.push(returnRandBase())
  }
  return newStrand;
}

// Returns a random Specimen Number
const newSpecimenNum = () => {
  let num = Math.floor(Math.random() * 1000);
  return num;
}


////////////////// Factory Function ////////////////////
const pAequorFactory = (specimenNum, dna) => {
  return {
    _specimenNum: specimenNum,
    _dna: dna,
    mutate() {
      let randomBase = this._dna[Math.floor(Math.random() * this._dna.length)];
        //console.log('randomly chosen base = '+ randomBase);
      let randomBaseIndex = this._dna.indexOf(randomBase);
        //console.log(randomBaseIndex);

      if (randomBase === 'A') {randomBase = 'T'}
        else if (randomBase === 'G') {randomBase = 'A'}
        else if (randomBase === 'T') {randomBase = 'C'}
        else if (randomBase === 'C') {randomBase = 'G'};

      this._dna.splice(randomBaseIndex, 1, randomBase);
      return this._dna;
    },
    compareDNA(pA1, pA2) {
      const commonBases = (pA1, pA2) => pA1.filter(item => pA2.includes(item));
      const percentSame = (commonBases.length / pA1.length) * 100 + '%';
      return 'These specimens have ' + percentSame + ' DNA in common.';
    },
    willLikelySurvive(dna) {
      let numSurvivalBases = [];
      let percentSurvivalBases;
      for (s = 0; s < dna.length; s++) {
        if (dna[s] === 'C' || dna[s] === 'G') {
          numSurvivalBases.push(dna[s]);
        }
      }
      percentSurvivalBases = (numSurvivalBases.length / dna.length) * 100;
        //console.log(percentSurvivalBases + '%');
      if (percentSurvivalBases > 60) {return true;} else {return false};
    },
  }
};


const makeSurvivors = (numNeeded) => {
  let survivors = [];
  
  for (let k = 1; survivors.length <= numNeeded; k++) {
    let makepA = pAequorFactory(k, mockUpStrand());
    let willSurvive = pAequorFactory().willLikelySurvive(makepA._dna);

    if (makepA && willSurvive) {
        survivors.push(makepA);
      } 
  }
  return survivors;
}
console.log(makeSurvivors(30));








////////////// Testing functions before adding them into the object
////////////// MUTATE //////////////
/*
const mutate = () => {
    let dna = [ 'C', 'G', 'C', 'A', 'G', 'G', 'A', 'G', 'A', 'A', 'G', 'G', 'A', 'T', 'A' ];
    let randomBase = dna[Math.floor(Math.random() * dna.length)];
      //console.log('randomly chosen base = '+ randomBase);
    let randomBaseIndex = dna.indexOf(randomBase);
      //console.log(randomBaseIndex);

    if (randomBase === 'A') {randomBase = 'T'}
    else if (randomBase === 'G') {randomBase = 'A'}
    else if (randomBase === 'T') {randomBase = 'C'}
    else if (randomBase === 'C') {randomBase = 'G'};

    dna.splice(randomBaseIndex, 1, randomBase);
    return dna;
}
//console.log(mutate());


////////////// COMPARING PAEQUOR OBJECTS //////////////
const compareDNA = (pA1, pA2) => {
  const commonBases = (pA1, pA2) => pA1.filter(item => pA2.includes(item));
  const percentSame = (commonBases.length / pA1.length) * 100 + '%';
  return 'These specimens have ' + percentSame + ' DNA in common.';
};
// console.log(compareDNA(pAequorObject1._dna, pAequorObject2._dna));


////////////// CHANCE OF SURVIVAL ///////////////
const willLikelySurvive = (pAequor) => {
  let numSurvivalBases = [];
  let percentSurvivalBases;
  for (s = 0; s < pAequor.length; s++) {
    if (pAequor[s] === 'C' || pAequor[s] === 'G') {
      numSurvivalBases.push(pAequor[s]);
    }
  }
  percentSurvivalBases = (numSurvivalBases.length / pAequor.length) * 100;
    console.log(percentSurvivalBases + '%');
  if (percentSurvivalBases > 60) {return true;} else {return false};
}
//console.log(willLikelySurvive(pAequorObject2._dna));
