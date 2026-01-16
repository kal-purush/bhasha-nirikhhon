console.log("events.js, yo!");

let output = document.getElementById("output-target");
let section = document.getElementsByTagName("section");

document.getElementsByTagName("section")[0].style.fontWeight = "bold";
document.getElementsByTagName("section")[5].style.fontWeight = "bold";
document.getElementsByTagName("section")[5].style.fontStyle = "italic";


//TARGET SECTIONS TO PRINT TO OUTPUT WHEN CLICKED
		function clickedSection(click){
			// console.log("click", click);
			//this is taking the inner HTML and making element text
			let sectionText = click.target.innerHTML;
			output.innerHTML = `You clicked on the ${sectionText} section`
		};

		for (let i = 0; i < section.length; i++){
			//this gets .item(i) because it is NOT an array. It is an HTML collection
			section.item(i).addEventListener("click", clickedSection);
		};

//CHANGE TEXT IN OUTPUT ON H1 MOUSE OVER AND MOUSE OUT
		//if I did not specify which h1 tag, I would have had to do another for loop or forEach.
		let header = document.getElementsByTagName("h1")[0];

			header.addEventListener("mouseover", headerMouseOver);
			header.addEventListener("mouseout", headerMouseOut);

		function headerMouseOver(event){
			output.innerHTML = `You moved your mouse over the header`;
		};

		function headerMouseOut(event){
			output.innerHTML = `You left me!!`;
		}


//CHANGE OUTPUT CONTENT ON KEYUP OF TEXT TYPED INTO INPUT BOX
		let userInput = document.getElementById("keypress-input");

		userInput.addEventListener("keyup", function(input){
			//this is telling the funtion to targer the innerHTML with the value that is typed into the input
			output.innerHTML = input.target.value;
		});

//When you click the "Add color" button, the guinea-pig element's text color should change to blue.
let guineaPig = document.getElementById("guinea-pig");

document.getElementById("add-color")
	.addEventListener("click", function(){
		guineaPig.classList.toggle("add-color")

	});

//When you click the "Hulkify" button, the guinea-pig element's font size should become much larger.

document.getElementById("make-large")
	.addEventListener("click", function(){
		guineaPig.classList.toggle("hulkify")

	});

//When you click the "Capture it" button, the guinea-pig element should have a border added to it.
document.getElementById("add-border")
	.addEventListener("click", function(){
		guineaPig.classList.toggle("add-border")

	});
//When you click the "Rounded" button, the guinea-pig element's border should become rounded.

document.getElementById("add-rounding")
	.addEventListener("click", function(){
		guineaPig.classList.toggle("add-rounding")

	});













