for (var i = 0; i < 60; i++) {
  $('#destroy').click(function () {
	$.ajax({
	  url: 'https://api.github.com/users/octocat/orgs',
	  accepts: ['application/vnd.github.v3+json'],
	  success: function(){
		$('#status').html('['+Date()+']: disabled');
	  },
	  error: function(){
		$('#status').html('['+Date()+']: already disabled');
	  }
	});
  });
}