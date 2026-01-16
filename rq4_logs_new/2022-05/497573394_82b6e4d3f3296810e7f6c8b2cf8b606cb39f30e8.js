/* typing animation */

var typed=new Typed('.typing',{
	strings:["Web Designer","Web Developer","Graphic Designer","Mobile Developer"],
	typeSpeed:150,
	BackSpeed:80,
	loop:true
})

/* Nav  */

const nav=document.querySelectorAll('.nav li a')
nav.forEach((item)=>{
	item.addEventListener('click',()=>{
		if(item.classList.contains('active')){
			item.classList.remove('active')
		}
		else
		{
			item.classList.add('active')
		}
	})
})