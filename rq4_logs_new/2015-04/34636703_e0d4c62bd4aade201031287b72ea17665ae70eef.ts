
var miniMe = {
  age: 0
}

function sayHello( age: number, name: string ) {

  if ( age > miniMe.age ) {

    return 'Say Hello ' + name

  }

  return '*ehm*'

}

alert( sayHello( 666, 'Who-Must-Not-Be-Named' ) )