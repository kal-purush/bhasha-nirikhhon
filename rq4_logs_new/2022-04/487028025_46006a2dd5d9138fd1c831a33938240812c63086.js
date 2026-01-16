const Contact = () => {
	const content = document.getElementById("content")
	content.innerText = ""
	const div = document.createElement("div")
	div.innerText = "You're conntect to the Contact.js"
	content.appendChild(div)
}

export default Contact