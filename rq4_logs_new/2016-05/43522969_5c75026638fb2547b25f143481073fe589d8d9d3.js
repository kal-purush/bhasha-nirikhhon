var names = ["Sarah", "Daniel", "Amber", "Tony", "Taylor", "Heather", "Megan", "Casey", "Jillian", "Jake", "John", "Dakota", "Richard", "Randy", "Ross", "Jessica", "Lucas", "Chase", "Piper", "Brittney"],
  assigned = [],
  nameLength = names.length;

for (var i = 0; i < nameLength; i++) {
  var recipientName = generateRandomName(i);
  console.log(names[i], "gives to ", recipientName);
}

function generateRandomName(giverIndex){
  var name = '';
  var validName = false;

  while(!validName) {
    var indexNumber = randomIndex();

    if (giverIndex == indexNumber) {
      continue;
    }

    if (assigned.indexOf(indexNumber) == -1) {
      assigned.push(indexNumber);
      name = names[indexNumber];
      validName = true;
    }
  }

  return name;
}

function randomIndex() {
  return Math.floor(Math.random() * nameLength);
};