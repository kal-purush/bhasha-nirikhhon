$(function()
{
	$(document).click(function(e){
		var $target = $(e.target);
		if(!$target || $target.closest("#filter-year-1").length == 0)
			$(".dropdown-list").eq(0).hide();
		if(!$target || $target.closest("#filter-year-2").length == 0)
			$(".dropdown-list").eq(1).hide();
		if(!$target || ($target.closest(".dropdown-checkbox").length == 0 && $target.closest("#filter-conference").length == 0))
			$(".dropdown-checkbox").hide();
		e.stopPropagation();
	});
	$(".dropdown-list").each(function()
	{
		var width = $(this).parent().width();
		$(this).width(width);
	});
	$(".dropdown-checkbox").each(function()
	{
		var width = $(this).parent().width();
		$(this).width(width);
	});
	$(".dropdown-wrap > button").each(function(){
		$(this).on("click", function()
		{
			$(this).next().toggle();
		});
	});
	$(".dropdown-list a").each(function(){
	 	$(this).on("click", function(){
			var text = $(this).text();
			$(this).parent().prev().text(text);
			$(this).parent().prev().append("<i class=\"fa fa-chevron-down\" aria-hidden=\"true\"></i>");
			$(this).parent().hide();
		})
	});
	$(".dropdown-checkbox").on("change", function()
	{
		let checkboxes = $(this).find(".checkbox>input");
		var checkedData = [];
		for (var i = 0; i < checkboxes.length; i++)
		{
			if($(checkboxes[i]).is(":checked"))
			{
				checkedData.push($(checkboxes[i]).next().next().text().trim());
			}
		}
		var filterConference = $("#filter-conference");
		filterConference.text("");
		for (var i = 0; i < checkedData.length - 1; i++)
			filterConference.append(checkedData[i] + ", ");
		if(checkedData.length > 0)
			filterConference.append(checkedData[checkedData.length - 1]);
		else filterConference.append("All");
		filterConference.append("<i class=\"fa fa-chevron-down\" aria-hidden=\"true\"></i>");
	});
});