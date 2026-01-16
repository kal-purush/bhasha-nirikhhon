//Our function must return the name of the person who submitted (vegetables.submitter) 
//the vegetable with the highest ranking in the provided category.

/*const judgeVegetable = function (vegetables, metric) {
  for (i = 0; i <= vegetables.length; i++) {
    if (vegetables[i].redness === 10) {
      return vegetables[i].submitter;
    } else if (vegetables[i].redness === 8) {
      return vegetables[i].submitter;
    }
  }
}*/
const judgeVegetable = function (vegetables, metric) {
  let bestQuality = 0;
  let submitter = '';
  for (let i = 0; i < vegetables.length; i++) {
    if (vegetables[i][metric] > bestQuality) {
      bestQuality = vegetables[i][metric];
      submitter = vegetables[i].submitter;
    }
  }submitter = '' + submitter;
  return submitter;
}
const vegetables = [
  {
    submitter: 'Old Man Franklin',
    redness: 1,
    plumpness: 5
  },
  {
    submitter: 'Sally Tomato-Grower',
    redness: 2,
    plumpness: 8
  },
  {
    submitter: 'Hamid Hamidson',
    redness: 4,
    plumpness: 3
  }
]

const metric = 'redness'

console.log(judgeVegetable(vegetables, metric));