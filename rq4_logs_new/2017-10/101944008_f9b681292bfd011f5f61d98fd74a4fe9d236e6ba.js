/**
 * 
 */
$(document).ready(function () {
		$('#loginForm').submit(function (event) {
			event.preventDefault();
			
			var id = $('#id').val();
			var pwd = $('#pwd').val();

			$.post("/WebClass/bloglogin",
					{ id: id, pwd: pwd },
					function(data) {

						var myModal = $('#myModal');
						myModal.modal();
						console.log(data.msg)
						if(data.msg=="error"){
							myModal.find('.modal-title').text('로그인 실패');
							myModal.find('.modal-body').text('로그인에 실패했습니다.');
							
						} else {
							myModal.find('.modal-title').text('로그인 결과');
							myModal.find('.modal-body').text(data.id + '님 로그인 되었습니다.');
							$('#myModal').on('hide.bs.modal', function(){
								console.log('h');
								location.reload();
							})
						}
						
						
					});
		});
	 });
 
 $(document).ready(function () {
		$('#registerForm').submit(function (event) {
			event.preventDefault();
			
			var name = $('#name').val();
			var num = $('#num').val();
			

			$.post("http://httpbin.org/post",
					{ name: name, num: num},
					function(data) {
						var exampleModal = $('#myModal');
						exampleModal.modal();
						exampleModal.find('.modal-title').text('회원가입 결과');
						exampleModal.find('.modal-body').text(data.form.name + '님 회원가입 되었습니다.');
						$('#exampleModal').modal('hide');
						
					});
		});
	 });
 
 $(document).ready(function() {
	  // Custom 
	  var stickyToggle = function(sticky, stickyWrapper, scrollElement) {
	    var stickyHeight = sticky.outerHeight();
	    var stickyTop = stickyWrapper.offset().top;
	    if (scrollElement.scrollTop() >= stickyTop){
	      stickyWrapper.height(stickyHeight);
	      sticky.addClass("is-sticky");
	    }
	    else{
	      sticky.removeClass("is-sticky");
	      stickyWrapper.height('auto');
	    }
	  };
	  
	  // Find all data-toggle="sticky-onscroll" elements
	  $('[data-toggle="sticky-onscroll"]').each(function() {
	    var sticky = $(this);
	    var stickyWrapper = $('<div>').addClass('sticky-wrapper'); // insert hidden element to maintain actual top offset on page
	    sticky.before(stickyWrapper);
	    sticky.addClass('sticky');
	    
	    // Scroll & resize events
	    $(window).on('scroll.sticky-onscroll resize.sticky-onscroll', function() {
	      stickyToggle(sticky, stickyWrapper, $(this));
	    });
	    
	    // On page load
	    stickyToggle(sticky, stickyWrapper, $(window));
	  });
	});
