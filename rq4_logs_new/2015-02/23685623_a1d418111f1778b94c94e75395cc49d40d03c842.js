function mudaAba(){
	$('#tab a').click(function (e) {
	  e.preventDefault()
	  $(this).tab('show')
	})
}