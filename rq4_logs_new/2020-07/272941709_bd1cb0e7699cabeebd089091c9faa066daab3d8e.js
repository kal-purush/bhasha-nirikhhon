const numbers = document.querySelectorAll(".number")

numbers.forEach((number) => {
	number.addEventListener("click", () => {
		console.log("number is pressed")
	})
})