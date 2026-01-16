function updateDepth(book, newPage) {

	var page = book.turn('page'),
		pages = book.turn('pages'),
		depthWidth = 16*Math.min(1, page*2/pages);

		newPage = newPage || page;

	if (newPage>5)
		$('.flipbook .p2 .depth').css({
			width: depthWidth,
			left: 20 - depthWidth
		});
	else
		$('.flipbook .p2 .depth').css({width: 0});

		depthWidth = 16*Math.min(1, (pages-page)*2/pages);

	if (newPage<pages-5)
		$('.flipbook .last-but-one .depth').css({
			width: depthWidth,
			right: 20 - depthWidth
		});
	else
		$('.flipbook .last-but-one .depth').css({width: 0});

}

function loadBook( page ) {
	
	var page = page;
    var flipbook = $('.flipbook');
	var flipbookViewport = $('.flipbook-viewport');
	
	
	if (!flipbook.turn('is')) {  
		flipbook.turn({
				elevation: 50,
				inclination: 50,
				autoCenter: true,
				gradients: true,
				duration: 1000,
		when: {
			turning: function(e, page, view) {
				
				var book = $(this),
					currentPage = book.turn('page'),
					pages = book.turn('pages');

				if (currentPage>3 && currentPage<pages-3) {
				
					if (page==1) {
						book.turn('page', 2).turn('stop').turn('page', page);
						e.preventDefault();
						return;
					} else if (page==pages) {
						book.turn('page', pages-1).turn('stop').turn('page', page);
						e.preventDefault();
						return;
					}
				} else if (page>3 && page<pages-3) {
					if (currentPage==1) {
						book.turn('page', 2).turn('stop').turn('page', page);
						e.preventDefault();
						return;
					} else if (currentPage==pages) {
						book.turn('page', pages-1).turn('stop').turn('page', page);
						e.preventDefault();
						return;
					}
				}

				updateDepth(book, page);
				
				if (page>=2)
					$('.flipbook .p2').addClass('fixed');
				else
					$('.flipbook .p2').removeClass('fixed');

				if (page<book.turn('pages'))
					$('.flipbook .last-but-one').addClass('fixed');
				else
					$('.flipbook .last-but-one').removeClass('fixed');
					
			},

			turned: function(e, page, view) {

				var book = $(this);

				if (page==2 || page==3) {
					book.turn('peel', 'br');
				}

				updateDepth(book);
				
				book.turn('center');
			},

			end: function(e, pageObj) {
			
				var book = $(this);
				updateDepth(book);
			}
		}
		
	 })
		
	}
	
		if (!flipbookViewport.hasClass('open')){
			$("#editorial .el-libro").removeClass("inactive").addClass("active")
			flipbookViewport.addClass('animated-book');
			flipbookViewport.removeClass('close').addClass('open');
		}
		
		if ( !page ) { var page = 4 }
		$(".flipbook").turn("page", page);
		
}


function closeBook() {

    var flipbook = $('.flipbook');
	var flipbookViewport = $('.flipbook-viewport');
	
	flipbookViewport.removeClass('open').addClass('close');
	
    flipbook.turn("page", 1);
}


/* ACCIONES */


$('body').on('click','.gotoPage',function(){
	var thePage = $(this).data("page")
	loadBook( thePage ) 
});


	
$(".el-libro a.back").on("click", function(e){
  
  closeBook();
  
  setTimeout(function(){
	  $("#editorial .el-libro").removeClass("active").addClass("inactive")
  }, 500);
  
})