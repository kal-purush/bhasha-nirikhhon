const head = document.querySelector("#head")
const neck = document.querySelector("#neck")
const body = document.querySelector("#body")
const leftArm = document.querySelector("#left-arm")
const rightArm = document.querySelector("#right-arm")
const arms = document.querySelectorAll(".arm")
const leftHand = document.querySelector("#left-hand")
const rightHand = document.querySelector("#right-arm")
const hands = document.querySelectorAll(".hand")
const leftLeg = document.querySelector("#left-leg")
const rightLeg = document.querySelector("#right-leg")
const legs = document.querySelectorAll(".leg")
const leftFoot = document.querySelector("#left-foot")
const rightFoot = document.querySelector("#right-foot")
const feet = document.querySelectorAll(".foot")

function paint(){
	document.querySelectorAll('.background>div').forEach(div => div.style.backgroundColor = document.querySelector('input[name="color"]').value)
	
}

paint()

function walk(){
	document.querySelectorAll(".arm,.hand,.leg,.foot").forEach(e => e.classList.add("upAndDown"))
	document.querySelectorAll("#right-arm,#right-hand,#left-leg,#left-foot").forEach(e => e.classList.add("delay"))
}

function blink(){
	document.querySelector("#left-eye").classList.add("blink")
}

function beat(){
	document.querySelector("#heart").classList.add("zoomInOut")
}

function eat(){
	document.querySelector("#lip").classList.add("shrinkUp")
}

function greet(){
	document.querySelector("#left-hand").classList.add("hide")
	document.querySelector("#left-arm").classList.add("leftLift")
	let style = document.createElement('STYLE')
	style.innerText = `.leftLift.arm::after{
		background-color: ${document.querySelector("#left-hand").style.backgroundColor}
	}`
	document.querySelector("head").appendChild(style)
}

function jump(){
	document.querySelector("#left-hand").classList.add("hide")
	document.querySelector("#left-arm").classList.add("leftLift")
	document.querySelector("#right-hand").classList.add("hide")
	document.querySelector("#right-arm").classList.add("rightLift")
	document.querySelector("#left-foot").classList.add("hide")
	document.querySelector("#left-leg").classList.add("leftLift")
	document.querySelector("#right-foot").classList.add("hide")
	document.querySelector("#right-leg").classList.add("rightLift")
	let style = document.createElement('STYLE')
	style.innerText = `#left-arm::after,#right-arm::after,#left-leg::after,#right-leg::after{
		background-color: ${document.querySelector("#left-hand").style.backgroundColor}
	}`
	document.querySelector("head").appendChild(style)
}

function dance(){
	document.querySelector("#left-hand").classList.add("hide")
	document.querySelector("#left-arm").classList.add("leftLift")
	document.querySelector("#right-hand").classList.add("hide")
	document.querySelector("#right-arm").classList.add("leftLift")
	document.querySelector("#right-leg").classList.add("rightLift")
	document.querySelector("#right-foot").classList.add("hide")

	let style = document.createElement('STYLE')
	style.innerText = `#left-arm::after,#right-arm::after,#left-leg::after,#right-leg::after{
		background-color: ${document.querySelector("#left-hand").style.backgroundColor}
	}`
	document.querySelector("head").appendChild(style)
}

function stop(classArr){
	document.querySelectorAll(".background div").forEach(e => {
		for(let i = 0; i<classArr.length; i++){
			e.classList.remove(classArr[i])
		}
	})
	
}

function entrance(){
	document.querySelectorAll('#body,#neck').forEach(e => e.classList.add('fadeInUp'))
	head.classList.add('headEntrance')
	document.querySelectorAll('#left-arm,#left-hand').forEach(e => e.classList.add('leftEntrance'))
	document.querySelectorAll('#right-arm,#right-hand').forEach(e => e.classList.add('rightEntrance'))
	legs.forEach(e => e.classList.add('legEntrance'))
	feet.forEach(e => e.classList.add('feetEntrance'))

	setTimeout(function(){
		stop(['fadeInUp','headEntrance','leftEntrance','rightEntrance','legEntrance','feetEntrance'])
		greet()
		setTimeout(() => stop(['leftLift','hide']),1500)
	},5000)
	
}

entrance()

/*=================
===================
Event Handling
===================
=================*/



document.querySelector('#color').addEventListener("change",paint)

document.querySelector("#walk").addEventListener("change",event => event.target.checked ? walk() : stop(["upAndDown","delay"]))

document.querySelector("#blink").addEventListener("change", event => event.target.checked ? blink() : stop(["blink"]))

document.querySelector("#beat").addEventListener("change", event => event.target.checked ? beat() : stop(["zoomInOut"]))

document.querySelector("#eat").addEventListener("change", event => event.target.checked ? eat() : stop(["shrinkUp"]))

document.querySelector("#greet").addEventListener("change", event => event.target.checked ? greet() : stop(["leftLift","hide"]))

document.querySelector("#jump").addEventListener("change", event => event.target.checked ? jump() : stop(["leftLift","rightLift","hide"]))

document.querySelector("#dance").addEventListener("change", event => event.target.checked ? dance() : stop(["leftLift","rightLift","hide"]))

