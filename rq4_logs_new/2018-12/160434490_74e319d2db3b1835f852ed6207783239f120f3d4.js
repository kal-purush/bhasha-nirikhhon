			function getUrlParameter(name) {
				name = name.replace(/[\[]/, '\\[').replace(/[\]]/, '\\]');
				var regex = new RegExp('[\\?&]' + name + '=([^&#]*)');
				var results = regex.exec(window.location.search);
				return results === null ? '' : decodeURIComponent(results[1].replace(/\+/g, ' '));
			};
			$(document).ready(function () {
				$.get("https://intellgentcms.herokuapp.com/api/pages",data=>{
					console.log(data);
					let lastPages = data.pages.splice(4);
					data.pages.forEach(p=>{
						$("#page-menu-column-1").append(`<a  class='mbr-black text-black dropdown-item display-4 p-0 m-0' href='interna?name=${p.url}' aria-expanded="false">${p.name}</a>`);
					})
					lastPages.forEach(p=>{
						$("#page-menu-column-2").append(`<a class="mbr-black text-black dropdown-item display-4 p-0 m-0" href="interna?name=${p.url}" aria-expanded="false">${p.name}</a>`);
					})

				})
				$.get(`https://intellgentcms.herokuapp.com/api/page?url=${getUrlParameter("name")}`,data=>{
					//console.log(data);
					$("#title").html(data.page.name);
					$("#page-title").html(data.page.name);

					$("#banner").css("background",`url(${data.page.image})`);
					$("#banner-description").css("background",`url(${data.page.image})`);
					data.page.sections.forEach((s,i)=>{
						$("#sectioncontainer").append(`<div class = 'section' id= 'section${i}' ></div>`);
						$(`#section${i}`).append(`<p class = 'section-title'>${s.title}</p>`);
						$(`#section${i}`).append(`<h3 class='mbr-section-subtitle mbr-fonts-style mb-4 mbr-light display-5 section-content'>${s.content}</h3>`);
						$(`#section${i}`).append(`<div class = 'section-img-container'></div>`);

						s.images.forEach((img,j)=>{
							$(".section-img-container").append(`<img class = 'section-img' src = ${img}></img>`);


						})
					})

				})
			});
