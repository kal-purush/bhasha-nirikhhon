//create a program that will ask the users name, age, and reddit username.have it tell them the information back, in the format:
//your name is(blank), you are(blank) years old, and your username is(blank)
//for extra credit, have the program log this information in a file to be accessed later.

class values {
    name: string;
    username: string;
    age: number;
}

function getValues(): values {
    var items = new values();
    items.name = prompt("Tell me name"),
    items.username = prompt("Tell me username"),
    items.age = Number(prompt("Tell me age"))

    return items;
}

function printGreeting(args: values): void {
    alert('your name is ' + args.name + ', you are '+ args.age +' years old, and your username is '+args.username)
}

var result: values = getValues();
printGreeting(result);