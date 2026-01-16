// this binds navigation items to items within the body 
// thanks https://jsfiddle.net/mekwall/up4nu/
function scrollspy(selector, heightOffset) {
	var lastId,
	    menu = $(selector),
	    menuItems = menu.find("a"),
	    scrollItems = menuItems.map(function(){
	      var item = $($(this).attr("href"));
	      if (item.length) { return item; }
	    });

	// Bind click handler to menu items so we can get a fancy scroll animation
	menuItems.click(function(e){
	  var href = $(this).attr("href"),
	      offsetTop = href === "#" ? 0 : $(href).offset().top+1;
    $("html, body").stop().animate({
      scrollTop: offsetTop
    }, 300);
    e.preventDefault();
	});

	// Bind to scroll
	$(window).scroll(function(){
	  // Get container scroll position
	  var fromTop = $(this).scrollTop();
   
	  // Get id of current scroll item
	  var cur = scrollItems.map(function(){
	    if ($(this).offset().top < fromTop)
	      return this;
	  });
	  // Get the id of the current element
	  cur = cur[cur.length-1];
	  var id = cur && cur.length ? cur[0].id : "";
   
	  if (lastId !== id) {
      lastId = id;
      menuItems
        .parent().removeClass("active")
        .end().filter("[href=#"+id+"]").parent().addClass("active");
	  }
	});	
}

// this brings up search by typing
function eversearch() {
	// Detect keystroke and present search
	var $searchField = $("#oncall .search .searchbox");
	
	function hideSearch(skipblur) { 
		$("#oncall").css("display","none");
		if (!skipblur) {
			$searchField.blur();
		}
	}
	
	function showSearch() {
		$("#oncall").css("display","block");
		$searchField.focus();	
	}
	
	$(window).keypress(function(e) {
		if ($(e.target).is('input, textarea')) {
			return;
		}
		
		var code = e.charCode || e.which;
		
		switch (code) {
			case 8:
			case 27:
				// do nothing
				break;
			default:
				showSearch();	
		}
	});
	
	$searchField.keydown(function(e) {
		var code = e.charCode || e.which;
		
		// exit upon esc
		if (code == 27) {
	 		hideSearch();
		}
		// exit upon delete
		if ((code == 8) && !$searchField.val()) {
			hideSearch();
		}
	});
	
	$searchField.blur(function(e) {
		$searchField.val("");
		hideSearch(true);
	});
}

// this filters items
function listfilter() {
	$("select.filter").change(function() {
		var selectedFilters = $.map($("select.filter").toArray(), function(e){
        return $(e).val();
		});
		var filteredFilters = selectedFilters.filter(function(filterItem) {
    	if (filterItem == "all") {
				return false;
			}
			else { 
				return true;
			}
    });
		filteredFilters.unshift("div");
		var filterSelector = filteredFilters.join(".");
    
		$("div#animal-list .col-3").find("div").addClass("filter-off");
    $("div#animal-list .col-3").find(filterSelector).removeClass("filter-off");
	});
}