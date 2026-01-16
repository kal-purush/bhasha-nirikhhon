var Person = function(config){
  this.name = config.name
  this.age = config.age
  this.job = config.job
}


Person.prototype.walk = function(){
  return this.name + 'walking...'
}


var nick = new Person({name: "Nick", age: "24", job: "Developer"});

console.log(nick.walk())