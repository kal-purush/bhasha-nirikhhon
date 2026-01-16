import reverseLookup from './index';

async function test() {
  const badresult = await reverseLookup('097626762');
  console.log(badresult);
  const goodresult = await reverseLookup('0977194800');
  console.log(goodresult);
}

test();