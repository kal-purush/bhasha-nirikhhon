let span=document.getElementsByTagName('span')[0];

function changeContent(){
	let prof=["UI Designer","UX Designer", "Programmer", "Web Developer", "App Developer"];
	let colors=["blue","green","pink","darkred","brown"];
	span.innerHTML=prof[Math.floor(Math.random()*5)];
	span.style.color=colors[(Math.floor(Math.random()*5))]
}

setInterval(changeContent, 4000);

document.getElementById('bar').addEventListener('click', ()=>{
	document.getElementsByTagName('ul')[0].style.display='block';
	document.getElementById('bar').style.display='none';
	document.getElementsByTagName('nav')[0].style.flexDirection='column';
	document.getElementsByTagName('nav')[0].style.marginLeft='4rem';
	document.getElementsByClassName('content')[0].style.marginTop='18rem';
	document.getElementsByClassName('container')[0].style.height='110vh';
})
