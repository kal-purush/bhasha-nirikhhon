var entries = require('object.entries')
const numArea = 10
const numTilePerArea = 24

const objMap = {
  grass: {
    rarity: 0.3,
    types:
    [
     { name: 'grass1', rarity: 0.6, numTile: 1 }
    ]
  },
  flower: {
    rarity: 0.2,
    types:
    [
     { name: 'flower4', rarity: 0.64, numTile: 1 },
     { name: 'flower1', rarity: 0.01, numTile: 1 },
     { name: 'flower3', rarity: 0.25, numTile: 1 },
     { name: 'flower2', rarity: 0.1, numTile: 1 }
    ]
  },
  twig: {
    rarity: 0.2,
    types:
    [
      { name: 'twig3', rarity: 0.6, numTile: 1 },
      { name: 'twig2', rarity: 0.3, numTile: 1 },
      { name: 'twig1', rarity: 0.1, numTile: 3 }
    ]
  },
  tree: {
    rarity: 0.1,
    types: [
      { name: 'tree1', rarity: 0.6, numTile: 3 },
      { name: 'tree2', rarity: 0.3, numTile: 3 },
      { name: 'tree3', rarity: 0.1, numTile: 5 }
    ]
  },
  rock: {
    rarity: 0.1,
    types:
    [
     { name: 'rock1', rarity: 0.8, numTile: 2 },
     { name: 'rock2', rarity: 0.2, numTile: 0.25 }
    ]
  },
  bush: {
    rarity: 0.1,
    types:
    [
     { name: 'bush1', rarity: 0.6, numTile: 1 }
    ]
  }
}
const objMapArray = entries(objMap)
console.log("\n objMapArray: "+objMapArray)
const findObj = (ranNum) =>{
  // extract keys in array-> [ 'tree', 'rock', 'bush', 'flower', 'twig', 'grass' ]
  let allObjNameArray = Object.keys(objMap)
  let sumRarity = 0
  for(let objNameKey in allObjNameArray)
  {
    for(let i=0; i< objMapArray.length; i++){
      if (allObjNameArray[objNameKey] === objMapArray[i][0]){
        sumRarity += objMapArray[i][1].rarity*100
        if (sumRarity >= ranNum) {
          return allObjNameArray[objNameKey]
        }
      }
    }
  }

}
const findObjType = (objName) =>{
  let sumRarity = 0
  let objectInArray = objMapArray.find( nameObj => nameObj[0] === objName)
  //console.log('re : '+objectInArray[1].types+ '\n')
  let ranNum = Math.floor(Math.random() * 100) + 1
  for(let i in objectInArray[1].types){
    sumRarity += objectInArray[1].types[i].rarity*100
    if (sumRarity >= ranNum) {
      console.log(objectInArray[1].types[i])
      return objectInArray[1].types[i]
    }
  }
}

const generateObj = (numberOfArea, objectWorld) => {
  let objToShow = []

  // number of total tiles for create object
  let maxRanNumTile = 210 + Math.floor(Math.random() * 30) + 1
  console.log(maxRanNumTile)
  let getObjToShow = {}
  // crate obj
  let counterNumTile = 0
  while (counterNumTile < 2)
  {
    // random number of obj (1 - 100)
    let ranNumObj = Math.floor(Math.random() * 100) + 1
    let ranNumTypeObj = Math.floor(Math.random() * 100) + 1
    let getObjName = findObj(ranNumObj)
    getObjToShow = findObjType(getObjName)
    //console.log('obj: ' + ranNumObj + '   type: ' + ranNumTypeObj)
    console.log(' getObj2: ' + getObjToShow.name + '\n')
    
    objToShow.push(getObjToShow)
    counterNumTile++
    //counterNumTile += getObjToShow.numTile
  }
  console.log(objToShow)
  
}

generateObj(numArea, objMap)
