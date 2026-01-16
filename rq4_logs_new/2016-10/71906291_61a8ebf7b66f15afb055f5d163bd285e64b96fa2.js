	/********************************************************************************************************
												GENERAL
	*********************************************************************************************************/


$(document).ready( function () {

	//menu dropdown toggle
	$(".menu").click(function () {
		$(".menuList").toggle();
	});

	$("#menuToggleBox").mouseleave(function () {
		$(".menuList").css("display","none");
	});

	var itemOpt = {
		//Insert different item colors/view in array. 
			// Note: first idber in each array represents item's order on the page (position idber) starting from 0 and reading from left to write on each line. 
			//Remaining array pics are different item colors/view

		//Populate multiview (".option")
			//Apparel items list



	/********************************************************************************************************
												JS: APPEARAL PAGE
	*********************************************************************************************************/

		apparel : 	[{
						shortDescription: "Hottest Trend by Brittany",
						longDescription: "As the temperatures warm from the runway to the red carpet, we're getting inspired by the season's hottest trends. Get it while it's still here! by Brittany",
						cost: "$19.99",
						galleryImage: "../img/fashion3.png",
						multipleViews: ["../img/fashion3.png", "../img/fashion4.png", "../img/fashion4.png", "../img/fashion3.png", "../img/fashion3.png", "../img/fashion4.png"],
						id: 0
					}, {
						shortDescription: "Hottest Trend by Brittany",
						longDescription: "As the temperatures warm from the runway to the red carpet, we're getting inspired by the season's hottest trends. Get it while it's still here! by Brittany",
						cost: "$19.99",
						galleryImage: "../img/fashion.png",
						multipleViews: ["../img/fashion.png", "../img/fashion2.png", "../img/fashion2.png", "../img/fashion.png", "../img/fashion.png", "../img/fashion2.png"],
						id: 1
					}, {
						shortDescription: "Hottest Trend by Brittany",
						longDescription: "As the temperatures warm from the runway to the red carpet, we're getting inspired by the season's hottest trends. Get it while it's still here! by Brittany",
						cost: "$19.99",
						galleryImage: "../img/fashion3.png",
						multipleViews: ["../img/fashion3.png", "../img/fashion4.png", "../img/fashion4.png", "../img/fashion3.png", "../img/fashion3.png", "../img/fashion4.png"],
						id: 2
					}, {
						shortDescription: "Hottest Trend by Brittany",
						longDescription: "As the temperatures warm from the runway to the red carpet, we're getting inspired by the season's hottest trends. Get it while it's still here! by Brittany",
						cost: "$19.99",
						galleryImage: "../img/fashion.png",
						multipleViews: ["../img/fashion.png", "../img/fashion2.png", "../img/fashion2.png", "../img/fashion.png", "../img/fashion.png", "../img/fashion2.png"],
						id: 3
					}, {
						shortDescription: "Hottest Trend by Brittany",
						longDescription: "As the temperatures warm from the runway to the red carpet, we're getting inspired by the season's hottest trends. Get it while it's still here! by Brittany",
						cost: "$19.99",
						galleryImage: "../img/fashion.png",
						multipleViews: ["../img/fashion.png", "../img/fashion2.png", "../img/fashion2.png", "../img/fashion.png", "../img/fashion.png", "../img/fashion2.png"],
						id: 4
					}, {
						shortDescription: "Hottest Trend by Brittany",
						longDescription: "As the temperatures warm from the runway to the red carpet, we're getting inspired by the season's hottest trends. Get it while it's still here! by Brittany",
						cost: "$19.99",
						galleryImage: "../img/fashion3.png",
						multipleViews: ["../img/fashion3.png", "../img/fashion4.png", "../img/fashion4.png", "../img/fashion3.png", "../img/fashion3.png", "../img/fashion4.png"],
						id: 5
					}, {
						shortDescription: "Hottest Trend by Brittany",
						longDescription: "As the temperatures warm from the runway to the red carpet, we're getting inspired by the season's hottest trends. Get it while it's still here! by Brittany",
						cost: "$19.99",
						galleryImage: "../img/fashion.png",
						multipleViews: ["../img/fashion.png", "../img/fashion2.png", "../img/fashion2.png", "../img/fashion.png", "../img/fashion.png", "../img/fashion2.png"],
						id: 6
					}, {
						shortDescription: "Hottest Trend by Brittany",
						longDescription: "As the temperatures warm from the runway to the red carpet, we're getting inspired by the season's hottest trends. Get it while it's still here! by Brittany",
						cost: "$19.99",
						galleryImage: "../img/fashion3.png",
						multipleViews: ["../img/fashion3.png", "../img/fashion4.png", "../img/fashion4.png", "../img/fashion3.png", "../img/fashion3.png", "../img/fashion4.png"],
						id: 7
					}],  //  apparel end


		populatePage : function () {

			var apparelSelection = "<h1 id='pageName'>Fashion Apparel</h1>";
			
			//populate page with apparel
			for ( i = 0 ; i < itemOpt.apparel.length ; i++ ) {
				var pic = itemOpt.apparel[i].galleryImage;
				var shortDescription = itemOpt.apparel[i].shortDescription;
				var cost = itemOpt.apparel[i].cost;

				apparelSelection += "<div class='appar imgGal'>";
				apparelSelection += "<img class='pic' src="+pic+">";
				apparelSelection += "<br>";
				apparelSelection += "<span class='dscrpt'>"+shortDescription+"</span>";
				apparelSelection += "<br>";
				apparelSelection += "<span class='cost'>"+cost+"</span>";
				apparelSelection += "<div class='likeButtonOne broken'>";
				apparelSelection += "<img src='../img/redHeart.png'>";
				apparelSelection += "</div>";
				apparelSelection += "<div class='likeButtonTwo fixed fHeart'>";
				apparelSelection += "<img src='../img/redHeart.png'>";
				apparelSelection += "</div>";
				apparelSelection += "</div>";
			}		

			$("#itemMenu").html(apparelSelection);

		},
					
				



	/********************************************************************************************************
												JS: BAG PAGE
	*********************************************************************************************************/



		bagItems : [
					[0, "../img/fashionbag.png" , "../img/fashionbag2.png", "../img/fashionbag2.png", "../img/fashionbag.png", "../img/fashionbag.png", "../img/fashionbag2.png"],
					[1, "../img/fashionbag3.png", "../img/fashionbag4.png", "../img/fashionbag4.png", "../img/fashionbag3.png", "../img/fashionbag3.png", "../img/fashionbag4.png"],
					[2, "../img/fashionbag.png", "../img/fashionbag2.png", "../img/fashionbag2.png", "../img/fashionbag.png", "../img/fashionbag.png", "../img/fashionbag2.png"],
					[3, "../img/fashionbag3.png", "../img/fashionbag4.png", "../img/fashionbag4.png", "../img/fashionbag3.png", "../img/fashionbag3.png", "../img/fashionbag4.png"],
					[4, "../img/fashionbag3.png", "../img/fashionbag4.png", "../img/fashionbag4.png", "../img/fashionbag3.png", "../img/fashionbag3.png", "../img/fashionbag4.png"],
					[5, "../img/fashionbag.png", "../img/fashionbag2.png", "../img/fashionbag2.png", "../img/fashionbag.png", "../img/fashionbag.png", "../img/fashionbag2.png"],
					[6, "../img/fashionbag3.png", "../img/fashionbag4.png", "../img/fashionbag4.png", "../img/fashionbag3.png", "../img/fashionbag3.png", "../img/fashionbag4.png"],
					[7, "../img/fashionbag.png", "../img/fashionbag2.png", "../img/fashionbag2.png", "../img/fashionbag.png", "../img/fashionbag.png", "../img/fashionbag2.png"]
				], // bagItems end



	/********************************************************************************************************
												GENERAL
	*********************************************************************************************************/



		init: function () {
			itemOpt.populatePage();
			itemOpt.assign();
		},  // init end
		
		assign: function () {

			//assign position numbers to each ".imgGal"
			$(".imgGal").each(function (index) {
				$(this).attr("data-id", index);
			});  // $(".imgGal").each end

			//assign false value to fixed heart
			$(".fixed img").attr("data-fixed", false);

			itemOpt.click();

			itemOpt.clickOff();

			

		},  // assign end

		click: function () {

			//turn on the heart button
			$(".broken img").click(function () {
				$(this).css("display", "none");
				$(this).parent().siblings(".fixed").children().fadeIn().attr("data-fixed", true);
			});  // $(".broken img").click end

			//turn off the heart button
			$(".fixed img").click(function () {
				$(this).css("display", "none").attr("data-fixed", false);
				$(this).parent().siblings(".broken").children().fadeIn();
			});  // $(".fixed img").click end

			

	/********************************************************************************************************
												JS: APPEARAL PAGE
	*********************************************************************************************************/



			//Used to: match the heart highlight in the gallery to the one in the multiview
			$(".pic").click(function () {
				var $dataid = $(this).parent().data("id");

				//change expanded apparel view in the multiview (".option") window with the (smaller) options in the side gallery
				$(".itemContainer .itemDisplay .options div img").click( function () {
					var $img_png = $(this).attr("src");
					$(".itemDisplay .img img").attr("src", $img_png);
				});

				// $(itemOpt.apparelItems).each(function (index) {
				// 	//The different styles for the item are placed inside the display box
				// 	if (itemOpt.apparelItems[index][0] === $dataid) {
				// 		for ( i = 1 ; i <= 6 ; i++ ){
				// 			$(".options .apparelPicCont"+i+" .apparelPic").attr("src", itemOpt.apparelItems[index][i])
				// 		}
				// 		//Set expanded apparel view to the first item in the array
				// 		$(".img img").attr("src",itemOpt.apparelItems[index][1]);
				// 	}
				// });

				$(itemOpt.apparel).each(function (index) {
					//The different styles for the item are placed inside the display box
					if (itemOpt.apparel[index].id === $dataid) {
						for ( i = 0 ; i < 6 ; i++ ){
							$(".options .apparelPicCont"+i+" .apparelPic").attr("src", itemOpt.apparel[index].multipleViews[i])
						}
						//Set expanded apparel view to the first item in the array
						$(".img img").attr("src",itemOpt.apparel[index].galleryImage);
					}
				});

				//activate the multiview window's heart if the gallery heart is activated
				actv = $(this).siblings(".fixed").children().attr("data-fixed");
				if(actv == "true"){
					$(".itemContainer .broken img").css("display","none");
					$(".itemContainer .broken").siblings(".fixed").children().fadeIn().attr("data-fixed", true);
				}
				//deactivate the multiview window's heart if the gallery heart is deactivated
				else{
					$(".itemContainer .fixed img").css("display","none").attr("data-fixed", false);
					$(".itemContainer .fixed").siblings(".broken").children().fadeIn();
				}
				
				//Toggle multiview (".option") window with gallery
				$(".appar").hide();
				$(".itemContainer").fadeIn(500).css("display", "block");
				$("#exitTarget").css("display", "block");

				//marks the multiview gallery with the pic data-id
				var $mark = $(this).parent().attr("data-id");
				$(".itemContainer").attr("marker", $mark);

				itemOpt.clickOff();
			});  // $(".pic").click end



	/********************************************************************************************************
												JS: BAG PAGE
	*********************************************************************************************************/



			//Used to: match the heart highlight in the gallery to the one in the multiview
			$(".picbag").click(function(){
				var $dataid = $(this).parent().data("id");

				//change expanded apparel view in the multiview (".option") window with the (smaller) options in the side gallery
				$(".itemContainer .itemDisplay .options div img").click(function(){
					var $img_png = $(this).attr("src");
					$(".itemDisplay .img img").attr("src", $img_png);
				});

				$(itemOpt.bagItems).each(function(index){
					//The different styles for the item are placed inside the display box
					if(itemOpt.bagItems[index][0] === $dataid){
						for( i = 1 ; i <= 6 ; i++){
							$(".options .apparelPicCont"+i+" .apparelPic").attr("src", itemOpt.bagItems[index][i])
						}
						//Set expanded apparel view to the first item in the array
						$(".img img").attr("src", itemOpt.bagItems[index][1]);
					}
				});
				
				// IF: activate the multiview window's heart if the gallery heart is activated
				// ESLE: deactivate the multiview window's heart if the gallery heart is deactivated
				actv = $(this).siblings(".fixed").children().attr("data-fixed");
				if(actv == "true"){
					$(".itemContainer .broken img").css("display","none");
					$(".itemContainer .broken").siblings(".fixed").children().fadeIn().attr("data-fixed", true);
				} else{
					$(".itemContainer .fixed img").css("display","none").attr("data-fixed", false);
					$(".itemContainer .fixed").siblings(".broken").children().fadeIn();
					}

				//Toggle multiview (".option") window with gallery
				$(".appar").hide();
				$(".itemContainer").fadeIn(500).css("display", "block");
				$("#exitTarget").css("display", "block");

				//marks the multiview gallery with the pic data-id
				var $mark = $(this).parent().attr("data-id");
				$(".itemContainer").attr("marker", $mark);

				itemOpt.clickOff();
			});  // $(".picbag").click end
		},  // click end



	/********************************************************************************************************
												GENERAL
	*********************************************************************************************************/



		//Used to: exit the multiview window via clicking outside the window
		clickOff: function(){
			//Toggle multiview (".option") window with gallery
			$("#exitTarget").click(function(){
				$(this).hide();
				$(".itemContainer").hide().css("display", "none");
				$("#exitTarget").css("display", "none");
				$(".appar").fadeIn(500);

				//activate the gallery heart if the multiview window's heart is activated
				var actv = $(".itemContainer .fixed img").attr("data-fixed");
				if(actv == "true"){
					//use the mark assigned in click() to get the multiview window's associated pic (w/ mark = data-id)
					var $mark = $(".itemContainer").attr("marker");
					$(".imgGal[data-id *= " + $mark + "]").children(".broken").children("img").css("display", "none");
					$(".imgGal[data-id *= " + $mark + "]").children(".fixed").children("img").fadeIn().attr("data-fixed", true);
				}

				//deactivate the gallery heart if the multiview window's heart is deactivated
				else {
					var $mark = $(".itemContainer").attr("marker");
					$(".imgGal[data-id *= " + $mark + "]").children(".fixed").children("img").css("display", "none").attr("data-fixed", false);
					$(".imgGal[data-id *= " + $mark + "]").children(".broken").children("img").fadeIn();
				}
			});  // $("#exitTarget").click end
		}, // clickOff end
	}; // itemOpt end

	itemOpt.init();



/********************************************************************************************************
											SHOPPING CART
*********************************************************************************************************/	



	//Slide in the shopping cart into page when clicked
	$("div#shoppingCart").click( function () {
		//check to see if the shopping cart is empty
		if ( $("#shoppingCartWindow").contents().length == 5){
			//show the default items if the shopping cart is empty
			$("#shoppingCartWindow").children("h1, h2").show();
		}
		//hide the hearts and descriptions
		$(".likeButtonOne img, .likeButtonTwo img, span.dscrpt, span.cost").hide();
		//hide the picture borders
		$(".pic").css("border", "none");
		//display #exitShoppingCartWindow
		$("#exitShoppingCartWindow").css("display", "block");
		//Shrinks the .pic out of view
		$("img.pic, img.picbag").animate({height: 0, width: 0}, {duration: 500, complete: function (){
			//displays the shopping cart
			$("#shoppingCartWindow").css("display", "block");
			//Slide in the shopping cart
			$("#shoppingCartWindow").animate({left: "0", right: "0", margin: "auto"}, { duration: 800 });
		}});
		
	});  // $("div#shoppingCart").click end

	//Slide in the shopping cart out of page when clicked
	$("#exitShoppingCartWindow").click(function(){
		//Slide in the shopping cart off the screen to the left 
		$("#shoppingCartWindow").animate({left: "2800px"},{duration: 800, complete: function () {
			//hide #exitShoppingCartWindow
			$("#exitShoppingCartWindow").css("display", "none");
			//Bring the .pic back into view
			$("img.pic, img.picbag").animate({height: "25.15625em", width: "18.75em"}, {duration: 500, complete: function () {
				//show the hearts and descriptions 
				$("span.dscrpt, span.cost").show();
			}});
			//show the picture borders
			$(".pic").css("border", "2px solid #F20A0A");
		}});


		//contains the ids of the items in shopping cart
		var markArray = [];
		//fill the markArray with the data-id for each picture in the shopping cart
		$(".buySection").each( function () {
			var $data = $(this).attr("data-id");
			markArray.push($data);
		});

		//activate hearts associated with items the in shopping cart when the gallery comes back
		$(markArray).each( function (index) {
			var $markArray = markArray[index];
			$(".appar").each( function () {

				var $compareMark = $(this).attr("data-id");
				if ( $markArray === $compareMark) {
					$(".imgGal[data-id *= " + $compareMark + "]").children(".broken").children("img").css("display", "none");
					$(".imgGal[data-id *= " + $compareMark + "]").children(".fixed").children("img").fadeIn().attr("data-fixed", true);
				}
			});  // $(".appar").each end
		});  // $(markArray).each end

		$(".imgGal .fixed img").each( function () {
			var dataid = $(this).attr("data-fixed");
			if (dataid === "false") {
				$(this).css("display", "none").attr("data-fixed", false);
				$(this).parent().siblings(".broken").children("img").fadeIn();
			}
		});
	});  //  $("#exitShoppingCartWindow").click end
	
	//Build the html to be writen in the #shoppingCartWindow
	// var populateShoppingCartHelper = function ( id, img, dscrpt, price) {
	// 	var populateShoppingCart = "<div class='buySection' data-id="+id+">";
	// 	populateShoppingCart += "<div class='buySectionImg'>";
	// 	populateShoppingCart += "<img class='picCart' src="+img+">";
	// 	populateShoppingCart += "</div>";
	// 	populateShoppingCart += "<div class='buySectionDescrpt'>";
	// 	populateShoppingCart += "<p>"+dscrpt+"</p>";
	// 	populateShoppingCart += "</div>";
	// 	populateShoppingCart += "<div class='buySectionTotalPrice'>";
	// 	populateShoppingCart += "<div class='shoppingCartCost'>"+price+"</div>";
	// 	populateShoppingCart += "</div>";
	// 	populateShoppingCart += "</div>";

	// 	return populateShoppingCart;



		// var populateShoppingCartHelper = function ( id, img, dscrpt) {
		// var populateShoppingCart = "<div class='buySection' data-id="+id+" ng-controller='qc'>";

		// populateShoppingCart += "<div class='buySectionImg'>";
		// populateShoppingCart += "<img class='picCart' src="+img+">";
		// populateShoppingCart += "</div>";
		// populateShoppingCart += "<div class='buySectionDescrpt'>";
		// populateShoppingCart += "<div class='name'>"+dscrpt+"</div>";
		// populateShoppingCart += "<br>";
		// populateShoppingCart += "<span class='costcompile'></span>";
		// populateShoppingCart += "<br>";
		// populateShoppingCart += "<br>";
		// populateShoppingCart += "<span class='quant' style='color: white;'>Qty: </span>";
		// populateShoppingCart += "<img class='operator' src='../img/minus.png' ng-click='changeQuantDown()'>";
		// populateShoppingCart += "<input ng-model='quant' type='text' id='text' name='text_name' style='width: 44px;' />";
		// populateShoppingCart += "<img class='operator' src='../img/plus.png' ng-click='changeQuantUp()'>";
		// populateShoppingCart += "<div class='update' ng-click='update()'>Update</div>";
		// populateShoppingCart += "</div>";
		// populateShoppingCart += "<div class='buySectionTotalPrice'>";
		// populateShoppingCart += "<span class='totalcompile'></span>";
		// populateShoppingCart += "</div>";
		// populateShoppingCart += "</div>";
		// populateShoppingCart += "</div>";
		// 			// <div class='buySection' data-id="0" ng-controller='qc'>
		// 			// 	<div class="buySectionImg">
		// 			// 		<img class="picCart" src="../img/fashion3.png">
		// 			// 	</div>
		// 			// 	<div class="buySectionDescrpt">
		// 			// 		<div class="name">Hottest Trend by Brittany</div>
		// 			// 		<br>
		// 			// 		<div class="price">${{cost}}</div>
		// 			// 		<br>
		// 			// 		<br>
		// 			// 		<span class="quant" style="color: white;">Qty: </span>
		// 			// 		<img class="operator" src="../img/minus.png" ng-click="changeQuantDown()">
		// 			// 		<input ng-model="quant" type="text" id="text" name="text_name" style="width: 44px;" />
		// 			// 		<img class="operator" src="../img/plus.png" ng-click="changeQuantUp()">
		// 			// 		<div class="update" ng-click="update()">Update</div>

		// 			// 	</div>
		// 			// 	<div class="buySectionTotalPrice">
		// 			// 		<div class="shoppingCartCost">${{updated | number : 2}}</div>
		// 			// 	</div>
		// 			// </div> -->

		// 	return populateShoppingCart;
		// };

	//Populate the #shoppingCartWindow via .imgGal
	$(".imgGal .broken img").click( function () {
		//hide the default text if the shopping cart is not empty
		$("#shoppingCartWindow").children("h1, h2").hide();
		//select the id when the heart is clicked
		var id = $(this).parent().parent().attr("data-id");
		
		//cycle through itemOpt.apparel to match the image, description, and price
		for ( i = 0 ; i < itemOpt.apparel.length ; i++ ) {
				if(itemOpt.apparel[i].id == id){
					//select the img when the heart is clicked
					var img = itemOpt.apparel[i].galleryImage;
					//select the description when the heart is clicked
					var dscrpt = itemOpt.apparel[i].shortDescription;
					//select the price when the heart is clicked
					var price = itemOpt.apparel[i].cost;
				}
			}	
		
		//var populater = populateShoppingCartHelper( id, img, dscrpt, price);
		//$("#shoppingCartWindow").append(populater);
	});

	//Unpopulate the #shoppingCartWindow via .imgGal
	$(".imgGal .fixed img").click( function () {
		var $mark = $(this).parent().parent().attr("data-id");
		$(".buySection[data-id *= " + $mark + "]").remove();
		
	});

	//Populate the #shoppingCartWindow via .itemContainer
	$(".itemContainer .broken img").click( function () {
		//hide the default items if the shopping cart is not empty
		//$("#shoppingCartWindow").children("h1, h2").hide();
		//select the id when the heart is clicked
		var id = $(this).parent().parent().parent().attr("marker");

		//cycle through itemOpt.apparel to math the image, description, and price
		for ( i = 0 ; i < itemOpt.apparel.length ; i++ ) {
				if(itemOpt.apparel[i].id == id){
					//select the img when the heart is clicked
					var img = itemOpt.apparel[i].galleryImage;
					//select the description when the heart is clicked
					var dscrpt = itemOpt.apparel[i].shortDescription;
					//select the price when the heart is clicked
					var price = itemOpt.apparel[i].cost;
				}
			}	

		//var populater = populateShoppingCartHelper( id, img, dscrpt, price);
		//$("#shoppingCartWindow").append(populater);
	});

	//Unpopulate the #shoppingCartWindow via .itemContainer
	$(".itemContainer .fixed img").click( function () {
		var $mark = $(this).parent().parent().parent().attr("marker");
		$(".buySection[data-id *= " + $mark + "]").remove();
	});

	//$(".nextItem").clone().appendTo("#shoppingCartWindow");
	///////////////
	// $(".itemContainer .broken img, .imgGal .broken img").click(function(){
	// 	$(".nextItem").clone().appendTo("#shoppingCartWindow");
	// 	$(".nextItem").removeClass("nextItem");
	// 	$(".buySection:last-of-type").addClass("nextItem");
	// });
	

});