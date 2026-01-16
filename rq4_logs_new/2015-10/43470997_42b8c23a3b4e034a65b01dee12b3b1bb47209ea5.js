$(function(){
  var prsSubmitted = 0
  $("[data-github-pr-submitted]").each(function(){
    var self = $(this)
    var url = $(this).attr("data-github-pr-submitted")
    console.log(url)
    $.getJSON(url, function(res){
      console.log(res)
      if(res.github_pr_submitted){
	prsSubmitted++
	var string = "<a href='"+res.github_pr_submitted.html_url+"'>View Pull Request.</a>"
        if(res.github_pr_submitted.state == "open"){
	  string += "<small> (open) </small>"
	}
      }else{
        var string = "Missing." 
      }
      $("[data-percent-complete]").html(function(){
	return ((prsSubmitted / $("[data-github-pr-submitted").length) * 100) + "%"
      })
      $("[data-percent-incomplete]").html(function(){
	return 100 - ((prsSubmitted / $("[data-github-pr-submitted").length) * 100) + "%"
      })
      self.html(string)
    })
  })
})