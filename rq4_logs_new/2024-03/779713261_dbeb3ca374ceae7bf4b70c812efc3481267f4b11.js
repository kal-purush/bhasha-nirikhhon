let btn = document.getElementById('blog');
btn.addEventListener("click", ()=>{
	let url = window.location.href;
	if (url.includes("index.html")){
		// pass
	}
	else{
		window.location.href = "http://localhost:3000/index.html#blog"
	}	
});