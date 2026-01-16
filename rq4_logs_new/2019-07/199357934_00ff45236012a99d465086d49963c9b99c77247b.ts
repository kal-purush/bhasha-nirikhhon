function greet(person: string) {
	return "Hello " + person
}

let user = "Jane User"
// let user = [1, 2, 3]


document.body.textContent = greet(user)