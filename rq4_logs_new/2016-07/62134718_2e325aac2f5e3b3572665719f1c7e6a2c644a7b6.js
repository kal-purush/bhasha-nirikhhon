$(function() {
	var students = {};
	var checkPagination = true;
	var currentPage = 1;
	
	var _ctx = ($("meta[name='ctx']").attr("content")===undefined) ? "" : $("meta[name='ctx']").attr("content");

	// Prepend context path to all jQuery AJAX requests
	$.ajaxPrefilter(function( options, originalOptions, jqXHR ) {
	    if (!options.crossDomain) {
	        options.url = _ctx + options.url;
	    }
	});
	
	//TODO: TO FIND ALL GENERATION
	students.findAll = function(){
		$.ajax({ 
		    url: "/v1/api/admin/students/", 
		    type: 'GET', 
		    dataType: 'JSON', 
		    data:{
		    	"limit" : $("#SELECT_PER_PAGE").val(),
		    	"page" : currentPage,
		    	"name" : $("#txtSearch").val(),
		    	"course" : $("#selectCourse").val(),
		    	"shift" : $("#selectShift").val(),
		    	"coursetype" : $("#selecetCourseType").val()
		    },
		    beforeSend: function(xhr) {
	            xhr.setRequestHeader("Accept", "application/json");
	            xhr.setRequestHeader("Content-Type", "application/json");
	        },
		    success: function(response) { 
		    	if(response.CODE=="0000"){
		    		$("#STUDENT").html("");
		    		if(response.DATA.length > 0){
		    			$("#TotalRecords").html(response.PAGINATION.TOTAL_COUNT);
		    			$.each(response.DATA, function(key,value){
		    				response.DATA[key]["NO"] = (key+1)+((response.PAGINATION.PAGE-1) * response.PAGINATION.LIMIT);
						});
		    			$("#STUDENT_TEMPLATE").tmpl(response.DATA).appendTo("tbody#STUDENT");
		    			if(checkPagination){
		    				students.setPagination(response.PAGINATION.TOTAL_PAGES);
		    				checkPagination=false;
		    			}
		    		}else{
		    			$("#STUDENT").html("<tr style='text-align:center;'><td colspan='6'>NO CONTENT</td></tr>");
		    			$("#PAGINATION").html("");
		    		}
		    	}
		    },
		    error:function(data,status,err) { 
		        console.log("error: "+data+" status: "+status+" err:"+err);
		    }
		});			
	};
	students.findAll();
	//TODO: TO SET THE PAGINATION FOR THE GENERATION
	students.setPagination = function(totalPage){
    	$('#PAGINATION').bootpag({
	        total: totalPage,
	        page: currentPage,
	        maxVisible: 10,
	        leaps: true,
	        firstLastUse: true,
	        first: 'First',
	        last: 'Last',
	        wrapClass: 'pagination',
	        activeClass: 'active',
	        disabledClass: 'disabled',
	        nextClass: 'next',
	        prevClass: 'prev',
	        lastClass: 'last',
	        firstClass: 'first'
	    });
	};
	
	$("#PAGINATION").on("page", function(event, page){
    	checkPagination = false;
    	currentPage = page;
    	students.findAll();
    }); 
	
	//TODO: EVENT HANDLING ON THE PER PAGE CHANGE
	$("#SELECT_PER_PAGE").change(function(){
		$("#limitPage").html($("#SELECT_PER_PAGE").val());
		checkPagination = true;
		students.findAll();
	});
	$("#btnSearch").click(function(){
		checkPagination = true;
		currentPage = 1;
		students.findAll();
	});
});