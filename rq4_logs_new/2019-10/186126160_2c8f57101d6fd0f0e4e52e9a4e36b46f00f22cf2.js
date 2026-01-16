
const yearOfBirth = 1982;
console.log(yearOfBirth);

const yearFuture = 2020;
console.log(yearFuture);
 
const age = yearFuture - yearOfBirth

console.log('you wil be' + age + 'years old in' + yearFuture);


var assignment2 = 'a dog age calculator';

console.log(assignment2);

var dogYearOfBirth = 2015
console.log(dogYearOfBirth);

var dogYearFuture = 2020
console.log(dogYearFuture);

var dogYear = dogYearFuture - dogYearOfBirth
console.log(dogYear);

var shouldShowResultInDogYears = true
 console.log(shouldShowResultInDogYears);

 console.log('your dog will be' + dogYear+ 'human years old in' + dogYearFuture  );

 if (shouldShowResultInDogYears) { 
     var dogAge = dogYear*5;
     console.log('your dog will be' + dogAge + 'dog years old in' + dogYearFuture);
     
 }


 var assignment3 = 'Housy' + 'pricy';
 console.log(assignment3);

 var peterVolumeInMeters1 =  8*10*10;
 var gardenSize1M2 = 100;
 console.log(peterVolumeInMeters1);

var peterHousePrice = peterVolumeInMeters1 * 2.5 * 1000 + gardenSize1M2 * 300;
console.log(peterHousePrice);
console.log("peter paid" + peterHousePrice);

 
if (peterHousePrice > 2500000) { 
    console.log("peter paid too much"); 
} 
else {
console.log("peter paid little");

}

 
 var juliaVolumeInMeters =  5*11*8;
 var gardenSize2M2 = 70;
 console.log(juliaVolumeInMeters);

 var juliaHousePrice = juliaVolumeInMeters * 2.5 * 1000 + gardenSize2M2 * 300;
console.log(juliaHousePrice);
console.log("Julia paid" + juliaHousePrice);

if (juliaHousePrice > 1000000) {
    console.log("Julia paid too much");
      
} else {
    console.log("Julia paid little");
    
}
