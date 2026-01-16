;
var siteRoot = '';

function ReloadProductDetailSlider(){
	 var galleryThumbs = new Swiper(".swiper-product-thumb .swiper-container", {
    spaceBetween: 20,
    freeMode: true,
    watchSlidesVisibility: true,
    slidesPerView: "4",
    watchSlidesVisibility: true,
    watchSlidesProgress: true
  });
  var galleryTop = new Swiper(".swiper-product-top .swiper-container", {
    effect: "fade",
    fadeEffect: {
      crossFade: true
    },
    thumbs: {
      swiper: galleryThumbs
    }
  });
}
function parse_query_string(query) {
	var vars = query.split("&");
	var query_string = {};
	for (var i = 0; i < vars.length; i++) {
		var pair = vars[i].split("=");
		var key = decodeURIComponent(pair[0]);
		var value = decodeURIComponent(pair[1]);
		// If first entry with this name
		if (typeof query_string[key] === "undefined") {
			query_string[key] = decodeURIComponent(value);
			// If second entry with this name
		} else if (typeof query_string[key] === "string") {
			var arr = [query_string[key], decodeURIComponent(value)];
			query_string[key] = arr;
			// If third or later entry with this name
		} else {
			query_string[key].push(decodeURIComponent(value));
		}
	}
	return query_string;
}

function setUrlParams(url, key, value) {
	url = url.split('?');
	usp = new URLSearchParams(url[1]);
	usp.set(key, value);
	url[1] = usp.toString();
	return url.join('?');
}
var txtkeyowrd = "#txtkeyword-productpage";
var txtkeyowrdCode = "#txtcode-productpage";
var beginTime2 = 0,
	endTime2 = 0;

function CheckTimer2() {
	var n2 = new Date;
	beginTime2 == 0 && endTime2 == 0 && (beginTime2 = n2.getTime());
	return
}

function SetTimeer2(n) {
	setTimeout(function () {
		var i = new Date,
			t = i.getTime();
		if (beginTime2 == endTime2) {
			if (t - endTime2 < 750 && t - endTime2 >= 0) {
				SetTimeer2(n);
				return
			}
			if (t - endTime2 >= 750) {
				AjaxCart.updatecart2();
				return
			}
		} else return
	}, n)
}
$(document).ready(function () {
	if($('body').hasClass("vi-vn"))
	{
		siteRoot="/vi-VN/";
	}
	$('body').on('click', '.btn-spin', function () {
		var $button = $(this);
		var oldValue = $button.parent().find('input').val();

		if ($button.text() == '+') {
			var newVal = parseFloat(oldValue) + 1;
		} else {
			// Don't allow decrementing below zero
			if (oldValue > 0) {
				var newVal = parseFloat(oldValue) - 1;
			} else {
				newVal = 0;
			}
		}
		if ($(this).parents('.cart-wrapper').length > 0) {
			SetTimeer2(1);
		}
		$button.parent().find('input').val(newVal);
		var sizeid = $button.parent().find('input').attr("data-size");
		if ($('.carttable').length)
			AjaxCart.updatecart(sizeid);
	});
	$("input[name='PaymentMethod']").click(function () {
		$(".method .sub-method").hide();
		if($(this).parent().find(".sub-method").length > 0)
		{
			$(this).parent().find(".sub-method").show(300);
			// if($(this).parent().find(".sub-method input:checked").length == 0)
			// {
			// 	$(this).parent().find(".sub-method input[type='radio']").first().prop('checked', true);
			// 	$(this).parent().find(".sub-method input[type='radio']:checked").trigger("change");
			// }
		}
	});
	var query_string = location.search.replace("?", "");
	var parsed_qs = parse_query_string(query_string);

	if (parsed_qs.keyword != undefined) {
		$(txtkeyowrd).val(parsed_qs.keyword);
	}
	if (parsed_qs.code != undefined) {
		$(txtkeyowrdCode).val(parsed_qs.code);
	}
	$("#txtkeyword-productpage").keyup(function (n) {
		var link = setUrlParams(window.location.href, "keyword", $(this).val());
		ProcessAjax(link);
	});
	$("#txtcode-productpage").keyup(function (n) {
		var link = setUrlParams(window.location.href, "code", $(this).val());
		ProcessAjax(link);
	});




	/*************************************************************************************************************/
	/* BEGIN SEARCH
	/*************************************************************************************************************/
	//Search button
	$(".productsearchbox input[type='submit'],.productsearchbox button").click(function () {
		var container = $(this).parent();
		if ($(container).find(".suggestsearch ul li.selected").length) {
			var n = $(container).find(".suggestsearch ul li.selected").eq(0).children("a").attr("href");
			window.location = n;

			return false;
		}

		if ($(container).find("input[type='text']").val() == '') {
			$(container).find("input[type='text']").focus();
			return false;
		}
	});

	//Search textbox
	$(".productsearchbox input[type='text']").keyup(function (n) {
		//        SuggestSearch(this, n);
		CallSuggest(this);
	});

	//    $(".productsearchbox input[type='text']").keydown(function () {
	//        CheckTimer();
	//    });

	var beginTime = 0,
		endTime = 0;

	function CheckTimer() {
		var n = new Date;
		beginTime == 0 && endTime == 0 && (beginTime = n.getTime());
		return
	}

	function SuggestSearch(obj, n) {
		var i = new Date,
			t;
		var suggestBoxItems = $(obj).parent().find(".suggestsearch ul");

		if (endTime = i.getTime(), beginTime = endTime, n.which == 40) {
			//$(".productsearchbox .suggestsearch ul li.selected").length == 0 ? $(".productsearchbox .suggestsearch ul li:first").addClass("selected") : (t = $(".productsearchbox .suggestsearch ul li.selected").next(), t.hasClass("li-group") && (t = t.next()), $(".productsearchbox .suggestsearch ul li.selected").removeClass("selected"), t.addClass("selected"));
			$(suggestBoxItems).find(".selected").length == 0 ? $(suggestBoxItems).find("li").first().addClass("selected") : (t = $(suggestBoxItems).find(".selected").next(), t.hasClass("li-group") && (t = t.next()), $(suggestBoxItems).find(".selected").removeClass("selected"), t.addClass("selected"));
			return
		}
		if (n.which == 38) {
			//$(".productsearchbox .suggestsearch ul li.selected").length == 0 ? $(".productsearchbox .suggestsearch ul li:last").addClass("selected") : (t = $(".productsearchbox .suggestsearch ul li.selected").prev(), t.hasClass("li-group") && (t = t.prev()), $(".productsearchbox .suggestsearch ul li.selected").removeClass("selected"), t.addClass("selected"));
			$(suggestBoxItems).find(".selected").length == 0 ? $(suggestBoxItems).find("li").last().addClass("selected") : (t = $(suggestBoxItems).find(".selected").prev(), t.hasClass("li-group") && (t = t.prev()), $(suggestBoxItems).find(".selected").removeClass("selected"), t.addClass("selected"));
			return
		}

		SetTimeer(obj, 1)
	}

	function SetTimeer(obj, n) {
		setTimeout(function () {
			var i = new Date,
				t = i.getTime();
			if (beginTime == endTime) {
				if (t - endTime < 750 && t - endTime >= 0) {
					SetTimeer(obj, n);
					return
				}
				if (t - endTime >= 750) {
					CallSuggest(obj);
					return
				}
			} else return
		}, n)
	}

	function CallSuggest(obj) {
	    //-----------------------------------------------------------------------------------------------------------------
        if ($('html').attr('lang') == "vi")
            siteRoot = '/vi-VN';
        //-----------------------------------------------------------------------------------------------------------------
		var container = $(obj).parent();
		if (!$(container).find(".suggestsearch").length)
			$(container).append("<div class='suggestsearch'></div>");

		var suggestBox = $(container).find(".suggestsearch");

		// var t = $(obj).val().replace(/:|;|!|@@|#|\$|%|\^|&|\*|'|"|>|<|,|\.|\?|\/|`|~|\+|=|_|\(|\)|{|}|\[|\]|\\|\|/gi, ""),
			// n = t.trim().toLowerCase();
			
		//var t = $(obj).val().replace(/:|;|!|@@|#|\$|%|\^|&|\*|'|"|>|<|,|\.|\?|\/|`|~|=|_|\(|\)|{|}|\[|\]|\\|\|/gi, ""),
			//n = t.trim().toLowerCase();
		var t = $(obj).val(),
            n = t.trim().toLowerCase();
		if (n.length < 2) {
			$(suggestBox).hide();
			return
		}
		n.length >= 2 && ($(obj).addClass("loading"), $.ajax({
			url: siteRoot + "/Product/Services/SuggestSearch.ashx",
			type: "GET",
			dataType: "html",
			data: {
				q: n
			},
			cache: !0,
			success: function (data) {
				$(obj).removeClass("loading");
				(data == "" || data == " ") ? $(suggestBox).hide(): ($(suggestBox).html(data).show())
			}
		}))
	}

	var obp = $(".searchresults .productcount");
	if ($(obp).length) {
		$.get(siteRoot + "/Product/Services/SearchCountResult.ashx?q=" + $(obp).attr("keyword"), function (data) {
			$(obp).text(" (" + data + ")");
		});
	}

	var obn = $(".searchresults .newscount");
	if ($(obn).length) {
		$.get(siteRoot + "/News/Services/SearchCountResult.ashx?q=" + $(obn).attr("keyword"), function (data) {
			$(obn).text(" (" + data + ")");
		});
	}

	/*************************************************************************************************************/
	/* END SEARCH
	/*************************************************************************************************************/

	/*************************************************************************************************************/
	/* BEGIN FILTER
	/*************************************************************************************************************/

	/********Price filter ********/
	minprice = $('.min-input').val();
	maxprice = $('.max-input').val();
	initSlider();

	function initSlider() {
		if ($('#slider-range').length > 0) {
			minValue = 0;
			maxValue = 500000000;
			if ($('.pricemin').text().length)
				minValue = parseInt($('.pricemin').text());
			if ($('.pricemax').text().length)
				maxValue = parseInt($('.pricemax').text());

			var options = {
				range: true,
				step: 5000000,
				min: 0,
				max: 500000000,
				values: [minValue, maxValue],
				slide: function (event, ui) {
					$(".price-range .min-input").html(addCommas(ui.values[0]));
					$(".price-range .max-input").html(addCommas(ui.values[1]));
				},
				change: function (event, ui) {
					if (minprice == ui.values[0] && maxprice == ui.values[1]) {
						// do nothing
					} else {
						minprice = ui.values[0];
						maxprice = ui.values[1];
						priceChange();
					}
				}
			};

			$("#slider-range").slider(options);

			$(".price-range .min-input").html(addCommas($("#slider-range").slider("values", 0)));
			$(".price-range .max-input").html(addCommas($("#slider-range").slider("values", 1)));
		}
	}

	function addCommas(nStr) {
		nStr += '';
		x = nStr.split('.');
		x1 = x[0];
		x2 = x.length > 1 ? '.' + x[1] : '';
		var rgx = /(\d+)(\d{3})/;
		while (rgx.test(x1)) {
			x1 = x1.replace(rgx, '$1' + '.' + '$2');
		}
		return x1 + x2;
	}

	function priceChange() {
		pageurl = $('.urlwithoutprice').attr('href');
		$.ajax({
			url: pageurl,
			data: {
				isajax: true,
				price: minprice + '-' + maxprice
			},
			success: function (data) {
				console.log(data)
				$('.ajaxresponse').html($(data).find('.ajaxresponse').html());
				$('.ajaxfilterresponse').html($(data).find('.ajaxfilterresponse').html());
				$('.ajaxbrandresponse').html($(data).find('.ajaxbrandresponse').html());
				$('.productpager').remove();
				$(data).find('.productpager').insertAfter($('.ajaxresponse'));

				pageurl = $(data).find('.urlwithprice').attr('href');
				//to change the browser URL to 'pageurl'
				if (pageurl != window.location) {
					window.history.pushState({
						path: pageurl
					}, '', pageurl);
				}
			}
		});

	}
	/********End Price filter ********/

	/********Reward points ********/
	initPointSlider();

	function initPointSlider() {
		var pointSlider = $('#pointSlider');
		if (pointSlider.length > 0) {
			var options = {
				range: false,
				step: pointSlider.data('step'),
				min: pointSlider.data('min'),
				max: pointSlider.data('max'),
				value: pointSlider.data('value'),
				slide: function (event, ui) {
					$("#spendingPointsLabel").text(ui.value);
					$("#spendingPoints").val(ui.value);
				},
				change: function (event, ui) {
					var data = {
						'method': 'SpendingPoint',
						'point': $("#spendingPoints").val()
					};
					$.ajax({
						cache: false,
						url: siteRoot + "/Product/Services/CheckoutService.aspx",
						data: data,
						type: 'post',
						success: function (result) {
							if (result.success) {
								$('.discount-total').html(result.discountTotal);
								$('.order-total').html(result.total);
							} else
								alert(result.message);
						}
					});
				}
			};

			pointSlider.slider(options);
		}
	}
	/********End Reward points ********/

	/****************/
	/* Ajax process */
	/****************/
	$("body").on("change", '.ajaxsort', function () {
		ProcessAjax($(this).val());
	});

	$("body").on("click", 'a.ajaxlink', function (e) {
		e.preventDefault();
		/*
		if uncomment the above line, html5 nonsupported browers won't change the url but will display the ajax content;
		if commented, html5 nonsupported browers will reload the page to the specified link.
		*/

		ProcessAjax($(this).attr('href'));

		//window.scrollTo(0, 0);
		if ($(this).parent().hasClass('reset_wrap'))
			initSlider();

		return false;
	});

	function ProcessAjax(pageurl) {
		//to get the ajax content and display in div with id 'content'
		$.ajax({
			url: pageurl,
			data: {
				isajax: true
			},
			success: function (data) {
				$('.ajaxresponse').html($(data).find('.ajaxresponse').html());
				$('.ajaxfilterresponse').html($(data).find('.ajaxfilterresponse').html());
				$('.ajaxbrandresponse').html($(data).find('.ajaxbrandresponse').html());
				$('.productpager').remove();
				$(data).find('.productpager').insertAfter($('.ajaxresponse'));
			}
		});

		//to change the browser URL to 'pageurl'
		if (pageurl != window.location) {
			window.history.pushState({
				path: pageurl
			}, '', pageurl);
		}
	}

	$("body").on("click", "a.ajaxpagerlink", function (e) {
		e.preventDefault();
		/*
		if uncomment the above line, html5 nonsupported browers won't change the url but will display the ajax content;
		if commented, html5 nonsupported browers will reload the page to the specified link.
		*/

		//get the link location that was clicked
		obj = $(this);
		pageurl = $(this).attr('href');

		//to get the ajax content and display in div with id 'content'
		$.ajax({
			url: pageurl,
			data: {
				isajax: true
			},
			success: function (data) {
				$('.ajaxresponse .ajaxresponsewrap').append($(data).find('.ajaxresponsewrap').html());
				obj.remove();
			}
		});

		//to change the browser URL to 'pageurl'
		if (pageurl != window.location) {
			window.history.pushState({
				path: pageurl
			}, '', pageurl);
		}

		//window.scrollTo(0, 0);

		return false;
	});

	/* the below code is to override back button to get the ajax content without reload*/
	$(window).bind('popstate', function () {
		$.ajax({
			url: location.pathname,
			data: {
				isajax: true
			},
			success: function (data) {
				$('.ajaxresponse').html($(data).find('.ajaxresponse').html());
				$('.ajaxfilterresponse').html($(data).find('.ajaxfilterresponse').html());
				$('.ajaxbrandresponse').html($(data).find('.ajaxbrandresponse').html());
			}
		});
	});

	/*************************************************************************************************************/
	/* END FILTER
	/*************************************************************************************************************/

	if ($('input[name="ShippingMethod"]').length) {
		$('input[name="ShippingMethod"]').trigger('change');
	}
})

/*************************************************************************************************************/
/* BEGIN CART
/*************************************************************************************************************/

var AjaxCart = {
	loadWaiting: false,
	usepopupnotifications: false,
	effecttocart: true,
	topcartselector: '.cart-wrapper .number .amount',
	topwishlistselector: '.wish-list-item .number',
	flyoutcartselector: '.cartpanel',

	init: function (usepopupnotifications, topcartselector, topwishlistselector, flyoutcartselector) {
		this.loadWaiting = false;
		this.usepopupnotifications = usepopupnotifications;
		this.topcartselector = topcartselector;
		this.topwishlistselector = topwishlistselector;
		this.flyoutcartselector = flyoutcartselector;
	},

	setLoadWaiting: function (display) {
		displayAjaxLoading(display);
		this.loadWaiting = display;
	},

	//add a product to the wishlist from the catalog pages
	addproducttowishlist: function (button) {
		if (this.loadWaiting != false) {
			return;
		}
		this.setLoadWaiting(true);

		var urladd = siteRoot + "/Product/Services/CartService.aspx";
		var data = {
			'method': 'AddProductToWishlist',
			'productid': $(button).attr('data-productid')
		};
		$.ajax({
			cache: false,
			url: urladd,
			data: data,
			type: 'post',
			success: function (response) {
				$(button).toggleClass("red");
				AjaxCart.success_desktop(response);
			},
			complete: this.resetLoadWaiting,
			error: this.ajaxFailure
		});
		return false;
	},
	//add a product to the cart/wishlist from the catalog pages
	addproducttocart_catalog: function (button) {
		if (this.loadWaiting != false) {
			return;
		}
		this.setLoadWaiting(true);

		if (this.effecttocart == true && this.topcartselector) {
			var img = $(button).parents('.product-item-category').find('.figure-image').find('img');
			flyToCartCatalog($(img), this.topcartselector);
		}
		var urladd = siteRoot + "/Product/Services/CartService.aspx";
		var data = {
			'method': 'AddProductToCart_Catalog',
			'productid': $(button).attr('data-productid')
		};
		$.ajax({
			cache: false,
			url: urladd,
			data: data,
			type: 'post',
			success:function (response) {
				$(button).parents().toggleClass("red");
				$(button).toggleClass("red");
				AjaxCart.success_desktop(response);
			} ,
			complete: this.resetLoadWaiting,
			error: this.ajaxFailure
		});
	},

	//add a product to the cart/wishlist from the product details page (desktop version)
	addproducttocart_details: function (button) {
		if (this.loadWaiting != false) {
			return;
		}
		this.setLoadWaiting(true);

		if (this.effecttocart == true && this.topcartselector) {
			var img = $('.swiper-product-top .swiper-slide-active img');
			flyToCart($(img), this.topcartselector);
		}

		var urladd = siteRoot + "/Product/Services/CartService.aspx";
		var data = $('#aspnetForm').serializeArray();
		data.push({
			name: 'method',
			value: 'AddProductToCart_Details'
		});
		data.push({
			name: 'productid',
			value: $(button).attr('data-productid')
		});
		$.ajax({
			cache: false,
			url: urladd,
			data: data,
			type: 'post',
			success: this.success_desktop,
			complete: this.resetLoadWaiting,
			error: this.ajaxFailure
		});
	},

	//add a product to the cart/wishlist from the product details page (mobile devices version)
	addproducttocart_details_mobile: function (successredirecturl) {
		if (this.loadWaiting != false) {
			return;
		}
		this.setLoadWaiting(true);

		var urladd = siteRoot + "/Product/Services/CartService.aspx";
		var data = $('#aspnetForm').serializeArray();
		data.push({
			name: 'method',
			value: 'AddProductToCart_Details_Mobile'
		});
		$.ajax({
			cache: false,
			url: urladd,
			data: data,
			type: 'post',
			success: function (response) {
				//if (response.updatetopcartsectionhtml) {
				//    $(AjaxCart.topcartselector).html(response.updatetopcartsectionhtml);
				//}
				//if (response.updatetopwishlistsectionhtml) {
				//    $(AjaxCart.topwishlistselector).html(response.updatetopwishlistsectionhtml);
				//}
				if (response.message) {
					//display notification
					if (response.success == true) {
						//we do not display success message in mobile devices mode
						//just redirect a user to the cart/wishlist
						location.href = successredirecturl;
					} else {
						//error
						displayStandardAlertNotification(response.message);
					}
					return false;
				}
				if (response.redirect) {
					location.href = response.redirect;
					return true;
				}
				return false;
			},
			complete: this.resetLoadWaiting,
			error: this.ajaxFailure
		});
	},

	//update cart
	updatecart: function () {
		if (this.loadWaiting != false) {
			return;
		}
		this.setLoadWaiting(true);

		var urladd = siteRoot + "/Product/Services/CartService.aspx";
		var data = $('#aspnetForm').serializeArray();
		data.push({
			name: 'method',
			value: 'UpdateCart'
		});
		$.ajax({
			cache: false,
			url: urladd,
			data: data,
			type: 'post',
			success: this.success_desktop,
			complete: this.resetLoadWaiting,
			error: this.ajaxFailure
		});
	},
	//update cart no load page
	updatecart2: function () {
		if (this.loadWaiting != false) {
			return;
		}
		this.setLoadWaiting(true);

		var urladd = siteRoot + "/Product/Services/CartService.aspx";
		var data = $('#aspnetForm').serializeArray();
		data.push({
			name: 'method',
			value: 'UpdateCart2'
		});
		$.ajax({
			cache: false,
			url: urladd,
			data: data,
			type: 'post',
			success: function (response) {
				$.ajax({
					url: window.location.href,
					data: {
						isajax: true
					},
					success: function (data) {
						$(".cart-wrapper span.amount").text($(data).find('.cart-wrapper span.amount').text());
						$('.cart-wrapper .cartpanel').html($(data).find('.cart-wrapper .cartpanel').html());
					}
				});
			},
			complete: this.resetLoadWaiting,
			error: this.ajaxFailure
		});
	},

	//remove from cart
	removefromcart: function (button) {
		if (this.loadWaiting != false) {
			return;
		}
		this.setLoadWaiting(true);

		var urladd = siteRoot + "/Product/Services/CartService.aspx";
		var data = {
			'method': 'RemoveFromCart',
			'itemguid': $(button).data('itemguid')
		};
		$.ajax({
			cache: false,
			url: urladd,
			data: data,
			type: 'post',
			//success: this.success_desktop,
			success: function () {
				location.reload();
			},
			complete: this.resetLoadWaiting,
			error: this.ajaxFailure
		});
	},

	//remove from cart
	removefromwishlist: function (button) {
		if (this.loadWaiting != false) {
			return;
		}
		this.setLoadWaiting(true);

		var urladd = siteRoot + "/Product/Services/CartService.aspx";
		var data = {
			'method': 'RemoveFromWishlist',
			'itemguid': $(button).data('itemguid')
		};
		$.ajax({
			cache: false,
			url: urladd,
			data: data,
			type: 'post',
			success: function () {
				location.reload();
			},
			complete: this.resetLoadWaiting,
			error: this.ajaxFailure
		});
	},

	//cart to wishlist
	carttowishlist: function (button) {
		if (this.loadWaiting != false) {
			return;
		}
		this.setLoadWaiting(true);

		var urladd = siteRoot + "/Product/Services/CartService.aspx";
		var data = {
			'method': 'CartToWishlist',
			'itemguid': $(button).data('itemguid')
		};
		$.ajax({
			cache: false,
			url: urladd,
			data: data,
			type: 'post',
			success: function () {
				location.reload();
			},
			complete: this.resetLoadWaiting,
			error: this.ajaxFailure
		});
	},

	// wishlist to cart
	wishlisttocart: function (button) {
		if (this.loadWaiting != false) {
			return;
		}
		this.setLoadWaiting(true);

		var urladd = siteRoot + "/Product/Services/CartService.aspx";
		var data = {
			'method': 'WishlistToCart',
			'itemguid': $(button).data('itemguid')
		};
		$.ajax({
			cache: false,
			url: urladd,
			data: data,
			type: 'post',
			success: function () {
				location.reload();
			},
			complete: this.resetLoadWaiting,
			error: this.ajaxFailure
		});
	},

	//apply coupon
	applycoupon: function () {
		if (this.loadWaiting != false) {
			return;
		}
		this.setLoadWaiting(true);

		var urladd = siteRoot + "/Product/Services/CartService.aspx";
		var data = {
			'method': 'ApplyCoupon',
			'couponcode': $('#couponCode').val()
		};
		$.ajax({
			cache: false,
			url: urladd,
			data: data,
			type: 'post',
			success: function (response) {
				if (response.success == false)
					$('.coupon-apply-error').text(response.message);
				else
					location.reload();
				//setLocation(response.redirect);
			},
			complete: this.resetLoadWaiting,
			error: this.ajaxFailure
		});
	},

	//remove coupon
	removecoupon: function () {
		if (this.loadWaiting != false) {
			return;
		}
		this.setLoadWaiting(true);

		var urladd = siteRoot + "/Product/Services/CartService.aspx";
		var data = {
			'method': 'RemoveCoupon'
		};
		$.ajax({
			cache: false,
			url: urladd,
			data: data,
			type: 'post',
			success: function (response) {
				location.reload();
				//setLocation(response.redirect);
			},
			complete: this.resetLoadWaiting,
			error: this.ajaxFailure
		});
	},

	success_desktop: function (response) {
		// var messageCart = 'Sản phẩm đã được đưa vào <a href="/cart">giỏ hàng</a>'
		// if (window.location.href.indexOf('en-US') > -1) {
		// 	messageCart = 'The product has been added to <a href="/en-US/shopping-cart">your cart</a>'
		// }
		// let noty = new Noty({
		//     type: 'success',
		//     layout: 'topRight',
		//     timeout: 3000,
		//     text: messageCart + ': ' + '<strong>' + $(response.updateflyoutcartsectionhtml).find('.last-added').text() + '</strong>'
		// }).show();
		if (response.updatetopcartsectionhtml) {
			$(AjaxCart.topcartselector).html(response.updatetopcartsectionhtml);
		}
		if (response.updatetopwishlistsectionhtml) {
			$(AjaxCart.topwishlistselector).html(response.updatetopwishlistsectionhtml);
		}
		if (response.updateflyoutcartsectionhtml) {
			var checkoutUrl = $('a.btn-checkout').attr('href');
			//$(AjaxCart.flyoutcartselector).replaceWith(response.updateflyoutcartsectionhtml);
			$(AjaxCart.flyoutcartselector).replaceWith($(response.updateflyoutcartsectionhtml).filter(AjaxCart.flyoutcartselector));
			$(AjaxCart.flyoutcartselector).addClass('active')
			setTimeout(() => {
				$(".cartpanel").removeClass("active");
			}, 3000);
			$(".btn-closecart").on("click", function () {
				$(".cartpanel").removeClass("active");
			});
			if (checkoutUrl != null)
				$(AjaxCart.flyoutcartselector).find('a.btn-checkout').attr('href', checkoutUrl);
		}
		if (response.message) {
			//display notification
			if (response.success == true) {
				//success
				if (AjaxCart.usepopupnotifications == true) {
					displayPopupNotification(response.message, 'success', true);
				} else {
					//specify timeout for success messages
					displayBarNotification(response.message, 'success', 3500);
				}
			} else {
				//error
				if (AjaxCart.usepopupnotifications == true) {
					displayPopupNotification(response.message, 'error', true);
				} else {
					//no timeout for errors
					displayBarNotification(response.message, 'error', 0);
				}

			}
			return false;
		}
		if (response.redirect) {
			setLocation(response.redirect);
			return true;
		}
		return false;
	},
 selectproductoption: function(button) {
        var url = siteRoot + "/Product/Services/CartService.aspx";
        //var data = $('#aspnetForm').serializeArray();
        var data = [];
        data.push({
            name: 'method',
            value: 'SelectProductOption'
        });
        data.push({
            name: 'productid',
            value: $("#hdProductId").val()
        });
        data.push({
            name: 'optionid',
            value: $(button).attr('data-id')
        });
        $.each($(button).parents('.product-attributes').find('.product-options'), function () {
            var input = $(this).find('input[type="hidden"]');
            if (input)
                data.push({ name: input.attr("name"), value: input.attr("value") });
        })
        $.ajax({
            cache: false,
            url: url,
            data: data,
            type: 'post',
            success: function(response) {
                if (response.success == true) {
                    $(button).parent().find('.product-option').removeClass('active');
                    $(button).parent().parent().find('.product-option-input').val($(button).attr('data-id'));

                    if (response.price)
                        $('.product-price').text(response.price);
                    if (response.oldPrice)
                        $('.product-oldprice').text(response.oldPrice);

                    if (response.productName)
                        $('.product-name').text(response.productName);
                    if (response.productCode)
                        $('.product-code').text(response.productCode);
                    if (response.briefContent)
                        $('.product-briefContent').html(response.briefContent);
                    // if (response.fullContent)
                    //     $('.product-fullContent').html(response.fullContent);
                    if (response.editLink)
                        $('.edit-link').attr('href', response.editLink);
                    if (response.metaTitle)
                        document.title = response.metaTitle;
                    if (response.metaDescription)
                        $('meta[name="description"]').attr("content", response.metaDescription);
                    if (response.childProductDetail) {
                        if ($(response.childProductDetail).filter(".ajaxproductslider") != undefined)
                            $('.ajaxproductslider').html($(response.childProductDetail).filter(".ajaxproductslider"))

                        if ($(response.childProductDetail).filter(".check-out-button") != undefined)
                            $(".button-wrapper").html($(response.childProductDetail).filter(".check-out-button"));

                        if ($(response.childProductDetail).filter(".tr.quantity") != undefined)
                            $(".productInfo .quantity").html($(response.childProductDetail).filter(".tr.quantity"));

                        if ($(response.childProductDetail).filter(".tr.status") != undefined)
                            $(".status-section").html($(response.childProductDetail).filter(".tr.status"));


                    }
                     if (response.selectedOptionIds) {
                        var optionNames = '';
                        var optionGroupCount = $(".product-info .product-options").length;
                        $(".product-options").each(function (index) {
                            $(this).find('.product-option-input').val('');
                            $(this).find('.product-option').removeClass('active');
                            $(this).find('.product-option').removeClass('disable');
                            $(this).find('.product-option').each(function (index2) {
                                var option = $(this);
                                var optionId = parseInt(option.attr('data-id'));
                                if ($.inArray(optionId, response.selectedOptionIds) > -1) {
                                    option.parent().parent().find('.product-option-input').val(optionId);
                                    option.addClass('active');

                                    if (option.parents('.product-intro').length)
                                        optionNames += option.attr('data-name') + ', ';
                                }
                                if (optionGroupCount > 1 && $.inArray(optionId, response.optionIds) == -1) {
                                    option.addClass('disable');
                                }
                            });

                        });

                        $('.product-selected-options .option-names').html(optionNames);
                    }
                    ReloadProductDetailSlider();
                    if (response.productUrl) {
                        if (typeof(history.pushState) != "undefined") {
                            var url = response.productUrl;
                            var obj = {
                                Title: '',
                                Url: url
                            };
                            history.pushState(obj, obj.Title, obj.Url);
                        }
                    }
                    $(button).addClass('active');
                    //lazyloadAjax();
                } else if (response.message) {
                    alert(response.message);
                }
                setTimeout(() => {
                    $('html,body').animate({
                        scrollTop: 200
                    }, 700);
                }, 300);
            },
            error: this.ajaxFailure
        });
    },
	resetLoadWaiting: function () {
		AjaxCart.setLoadWaiting(false);
	},

	ajaxFailure: function () {
		alert('Failed to add the product to the cart. Please refresh the page and try one more time.');
	}
}

function OpenWindow(n, t, i, r) {
	var u = (screen.width - t) / 2,
		f = (screen.height - i) / 2,
		e;
	winprops = "resizable=0, height=" + i + ",width=" + t + ",top=" + f + ",left=" + u + "w";
	r && (winprops += ",scrollbars=1");
	e = window.open(n, "_blank", winprops)
}

function setLocation(n) {
	window.location.href = n
}

function displayAjaxLoading(n) {
	n ? $(".ajax-loading-block-window").show() : $(".ajax-loading-block-window").hide("slow")
}

function displayPopupNotification(n, t, i) {
	var f, r, u, e;
	if (f = t == "success" ? $("#dialog-notifications-success") : t == "error" ? $("#dialog-notifications-error") : $("#dialog-notifications-success"), r = "", typeof n == "string") r = "<p>" + n + "<\/p>";
	else
		for (u = 0; u < n.length; u++) r = r + "<p>" + n[u] + "<\/p>";
	f.html(r);
	e = i ? !0 : !1;
	f.dialog({
		modal: e
	})
}

function closePopupNotification() {
	$(".ui-dialog-titlebar-close").trigger("click");
	return false;
}

function displayBarNotification(n, t, i) {
	var u, r, f;
	if (clearTimeout(barNotificationTimeout), u = "success", t == "success" ? u = "success" : t == "error" && (u = "error"), $("#bar-notification").removeClass("success").removeClass("error"), $("#bar-notification .content").remove(), r = "", typeof n == "string") r = '<p class="content">' + n + "<\/p>";
	else
		for (f = 0; f < n.length; f++) r = r + '<p class="content">' + n[f] + "<\/p>";
	$("#bar-notification").append(r).addClass(u).fadeIn("slow").mouseenter(function () {
		clearTimeout(barNotificationTimeout)
	});
	$("#bar-notification .close").unbind("click").click(function () {
		$("#bar-notification").fadeOut("slow")
	});
	i > 0 && (barNotificationTimeout = setTimeout(function () {
		$("#bar-notification").fadeOut("slow")
	}, i))
}

// fly to basket
function flyToCartCatalog(flyer, flyingTo, callBack) {
	try {
		var $jqfunc = $(this);
		var divider = 3;
		var flyerClone = $(flyer).clone();
		$(flyerClone).css({
			position: 'absolute',
			top: $(flyer).offset().top + "px",
			left: $(flyer).offset().left + "px",
			opacity: 1,
			'z-index': 1000
		});
		$('body').append($(flyerClone))
		if ($(flyingTo)) {
			var gotoX = $(flyingTo).offset().left + ($(flyingTo).width() / 2) - ($(flyer).width() / divider) / 2;
			var gotoY = $(flyingTo).offset().top + ($(flyingTo).height() / 2) - ($(flyer).height() / divider) / 2;
			$(flyerClone).animate({
					opacity: 0.7,
					left: gotoX,
					top: gotoY,
					width: 60,
					height: 60
				}, 1000,
				function () {
					$(flyingTo).fadeOut('slowly', function () {
						$(flyingTo).fadeIn('slowly', function () {
							$(flyerClone).fadeOut('slowly', function () {
								$(flyerClone).remove();
								if (callBack != null) {
									callBack.apply($jqfunc);
								}
							});
						});
					});
				});
		}

	} catch (exception) {

	}
}

function flyToCart(flyer, flyingTo, callBack) {
	try {
		var $jqfunc = $(this);
		var divider = 3;
		var flyerClone = $(flyer).clone();
		$(flyerClone).css({
			position: 'absolute',
			top: $(flyer).parents('.information-swiper-wrapper').offset().top + "px",
			left: $(flyer).parents('.information-swiper-wrapper').offset().left + "px",
			opacity: 1,
			'z-index': 1000
		});
		$('body').append($(flyerClone))
		if ($(flyingTo)) {
			var gotoX = $(flyingTo).offset().left + ($(flyingTo).width() / 2) - ($(flyer).width() / divider) / 2;
			var gotoY = $(flyingTo).offset().top + ($(flyingTo).height() / 2) - ($(flyer).height() / divider) / 2;
			$(flyerClone).animate({
					opacity: 0.7,
					left: gotoX,
					top: gotoY,
					width: 135,
					height: 135
				}, 1000,
				function () {
					$(flyingTo).fadeOut('slowly', function () {
						$(flyingTo).fadeIn('slowly', function () {
							$(flyerClone).fadeOut('slowly', function () {
								$(flyerClone).remove();
								if (callBack != null) {
									callBack.apply($jqfunc);
								}
							});
						});
					});
				});
		}

	} catch (exception) {

	}
}

function htmlEncode(n) {
	return $("<div/>").text(n).html()
}

function htmlDecode(n) {
	return $("<div/>").html(n).text()
}
var barNotificationTimeout, AjaxCart;

/*************************************************************************************************************/
/* END CART
/*************************************************************************************************************/

/*************************************************************************************************************/
/* BEGIN CHECKOUT
/*************************************************************************************************************/
/*! jQuery Validation Plugin - v1.14.0 - 6/30/2015
 * http://jqueryvalidation.org/
 * Copyright (c) 2015 Jörn Zaefferer; Licensed MIT */
!function(t){"function"==typeof define&&define.amd?define(["jquery"],t):t(jQuery)}(function(t){t.extend(t.fn,{validate:function(e){if(this.length){var i=t.data(this[0],"validator");return i||(this.attr("novalidate","novalidate"),i=new t.validator(e,this[0]),t.data(this[0],"validator",i),i.settings.onsubmit&&(this.on("click.validate",":submit",function(e){i.settings.submitHandler&&(i.submitButton=e.target),t(this).hasClass("cancel")&&(i.cancelSubmit=!0),void 0!==t(this).attr("formnovalidate")&&(i.cancelSubmit=!0)}),this.on("submit.validate",function(e){function s(){var s,r;return!i.settings.submitHandler||(i.submitButton&&(s=t("<input type='hidden'/>").attr("name",i.submitButton.name).val(t(i.submitButton).val()).appendTo(i.currentForm)),r=i.settings.submitHandler.call(i,i.currentForm,e),i.submitButton&&s.remove(),void 0!==r&&r)}return i.settings.debug&&e.preventDefault(),i.cancelSubmit?(i.cancelSubmit=!1,s()):i.form()?i.pendingRequest?(i.formSubmitted=!0,!1):s():(i.focusInvalid(),!1)})),i)}e&&e.debug&&window.console&&console.warn("Nothing selected, can't validate, returning nothing.")},valid:function(){var e,i,s;return t(this[0]).is("form")?e=this.validate().form():(s=[],e=!0,i=t(this[0].form).validate(),this.each(function(){e=i.element(this)&&e,s=s.concat(i.errorList)}),i.errorList=s),e},rules:function(e,i){var s,r,n,a,o,l,h=this[0];if(e)switch(s=t.data(h.form,"validator").settings,r=s.rules,n=t.validator.staticRules(h),e){case"add":t.extend(n,t.validator.normalizeRule(i)),delete n.messages,r[h.name]=n,i.messages&&(s.messages[h.name]=t.extend(s.messages[h.name],i.messages));break;case"remove":return i?(l={},t.each(i.split(/\s/),function(e,i){l[i]=n[i],delete n[i],"required"===i&&t(h).removeAttr("aria-required")}),l):(delete r[h.name],n)}return(a=t.validator.normalizeRules(t.extend({},t.validator.classRules(h),t.validator.attributeRules(h),t.validator.dataRules(h),t.validator.staticRules(h)),h)).required&&(o=a.required,delete a.required,a=t.extend({required:o},a),t(h).attr("aria-required","true")),a.remote&&(o=a.remote,delete a.remote,a=t.extend(a,{remote:o})),a}}),t.extend(t.expr[":"],{blank:function(e){return!t.trim(""+t(e).val())},filled:function(e){return!!t.trim(""+t(e).val())},unchecked:function(e){return!t(e).prop("checked")}}),t.validator=function(e,i){this.settings=t.extend(!0,{},t.validator.defaults,e),this.currentForm=i,this.init()},t.validator.format=function(e,i){return 1===arguments.length?function(){var i=t.makeArray(arguments);return i.unshift(e),t.validator.format.apply(this,i)}:(arguments.length>2&&i.constructor!==Array&&(i=t.makeArray(arguments).slice(1)),i.constructor!==Array&&(i=[i]),t.each(i,function(t,i){e=e.replace(new RegExp("\\{"+t+"\\}","g"),function(){return i})}),e)},t.extend(t.validator,{defaults:{messages:{},groups:{},rules:{},errorClass:"error",validClass:"valid",errorElement:"label",focusCleanup:!1,focusInvalid:!0,errorContainer:t([]),errorLabelContainer:t([]),onsubmit:!0,ignore:":hidden",ignoreTitle:!1,onfocusin:function(t){this.lastActive=t,this.settings.focusCleanup&&(this.settings.unhighlight&&this.settings.unhighlight.call(this,t,this.settings.errorClass,this.settings.validClass),this.hideThese(this.errorsFor(t)))},onfocusout:function(t){this.checkable(t)||!(t.name in this.submitted)&&this.optional(t)||this.element(t)},onkeyup:function(e,i){9===i.which&&""===this.elementValue(e)||-1!==t.inArray(i.keyCode,[16,17,18,20,35,36,37,38,39,40,45,144,225])||(e.name in this.submitted||e===this.lastElement)&&this.element(e)},onclick:function(t){t.name in this.submitted?this.element(t):t.parentNode.name in this.submitted&&this.element(t.parentNode)},highlight:function(e,i,s){"radio"===e.type?this.findByName(e.name).addClass(i).removeClass(s):t(e).addClass(i).removeClass(s)},unhighlight:function(e,i,s){"radio"===e.type?this.findByName(e.name).removeClass(i).addClass(s):t(e).removeClass(i).addClass(s)}},setDefaults:function(e){t.extend(t.validator.defaults,e)},messages:{required:"This field is required.",remote:"Please fix this field.",email:"Please enter a valid email address.",url:"Please enter a valid URL.",date:"Please enter a valid date.",dateISO:"Please enter a valid date ( ISO ).",number:"Please enter a valid number.",digits:"Please enter only digits.",creditcard:"Please enter a valid credit card number.",equalTo:"Please enter the same value again.",maxlength:t.validator.format("Please enter no more than {0} characters."),minlength:t.validator.format("Please enter at least {0} characters."),rangelength:t.validator.format("Please enter a value between {0} and {1} characters long."),range:t.validator.format("Please enter a value between {0} and {1}."),max:t.validator.format("Please enter a value less than or equal to {0}."),min:t.validator.format("Please enter a value greater than or equal to {0}.")},autoCreateRanges:!1,prototype:{init:function(){function e(e){var i=t.data(this.form,"validator"),s="on"+e.type.replace(/^validate/,""),r=i.settings;r[s]&&!t(this).is(r.ignore)&&r[s].call(i,this,e)}this.labelContainer=t(this.settings.errorLabelContainer),this.errorContext=this.labelContainer.length&&this.labelContainer||t(this.currentForm),this.containers=t(this.settings.errorContainer).add(this.settings.errorLabelContainer),this.submitted={},this.valueCache={},this.pendingRequest=0,this.pending={},this.invalid={},this.reset();var i,s=this.groups={};t.each(this.settings.groups,function(e,i){"string"==typeof i&&(i=i.split(/\s/)),t.each(i,function(t,i){s[i]=e})}),i=this.settings.rules,t.each(i,function(e,s){i[e]=t.validator.normalizeRule(s)}),t(this.currentForm).on("focusin.validate focusout.validate keyup.validate",":text, [type='password'], [type='file'], select, textarea, [type='number'], [type='search'], [type='tel'], [type='url'], [type='email'], [type='datetime'], [type='date'], [type='month'], [type='week'], [type='time'], [type='datetime-local'], [type='range'], [type='color'], [type='radio'], [type='checkbox']",e).on("click.validate","select, option, [type='radio'], [type='checkbox']",e),this.settings.invalidHandler&&t(this.currentForm).on("invalid-form.validate",this.settings.invalidHandler),t(this.currentForm).find("[required], [data-rule-required], .required").attr("aria-required","true")},form:function(){return this.checkForm(),t.extend(this.submitted,this.errorMap),this.invalid=t.extend({},this.errorMap),this.valid()||t(this.currentForm).triggerHandler("invalid-form",[this]),this.showErrors(),this.valid()},checkForm:function(){this.prepareForm();for(var t=0,e=this.currentElements=this.elements();e[t];t++)this.check(e[t]);return this.valid()},element:function(e){var i=this.clean(e),s=this.validationTargetFor(i),r=!0;return this.lastElement=s,void 0===s?delete this.invalid[i.name]:(this.prepareElement(s),this.currentElements=t(s),(r=!1!==this.check(s))?delete this.invalid[s.name]:this.invalid[s.name]=!0),t(e).attr("aria-invalid",!r),this.numberOfInvalids()||(this.toHide=this.toHide.add(this.containers)),this.showErrors(),r},showErrors:function(e){if(e){for(var i in t.extend(this.errorMap,e),this.errorList=[],e)this.errorList.push({message:e[i],element:this.findByName(i)[0]});this.successList=t.grep(this.successList,function(t){return!(t.name in e)})}this.settings.showErrors?this.settings.showErrors.call(this,this.errorMap,this.errorList):this.defaultShowErrors()},resetForm:function(){t.fn.resetForm&&t(this.currentForm).resetForm(),this.submitted={},this.lastElement=null,this.prepareForm(),this.hideErrors();var e,i=this.elements().removeData("previousValue").removeAttr("aria-invalid");if(this.settings.unhighlight)for(e=0;i[e];e++)this.settings.unhighlight.call(this,i[e],this.settings.errorClass,"");else i.removeClass(this.settings.errorClass)},numberOfInvalids:function(){return this.objectLength(this.invalid)},objectLength:function(t){var e,i=0;for(e in t)i++;return i},hideErrors:function(){this.hideThese(this.toHide)},hideThese:function(t){t.not(this.containers).text(""),this.addWrapper(t).hide()},valid:function(){return 0===this.size()},size:function(){return this.errorList.length},focusInvalid:function(){if(this.settings.focusInvalid)try{t(this.findLastActive()||this.errorList.length&&this.errorList[0].element||[]).filter(":visible").focus().trigger("focusin")}catch(t){}},findLastActive:function(){var e=this.lastActive;return e&&1===t.grep(this.errorList,function(t){return t.element.name===e.name}).length&&e},elements:function(){var e=this,i={};return t(this.currentForm).find("input, select, textarea").not(":submit, :reset, :image, :disabled").not(this.settings.ignore).filter(function(){return!this.name&&e.settings.debug&&window.console&&console.error("%o has no name assigned",this),!(this.name in i||!e.objectLength(t(this).rules()))&&(i[this.name]=!0,!0)})},clean:function(e){return t(e)[0]},errors:function(){var e=this.settings.errorClass.split(" ").join(".");return t(this.settings.errorElement+"."+e,this.errorContext)},reset:function(){this.successList=[],this.errorList=[],this.errorMap={},this.toShow=t([]),this.toHide=t([]),this.currentElements=t([])},prepareForm:function(){this.reset(),this.toHide=this.errors().add(this.containers)},prepareElement:function(t){this.reset(),this.toHide=this.errorsFor(t)},elementValue:function(e){var i,s=t(e),r=e.type;return"radio"===r||"checkbox"===r?this.findByName(e.name).filter(":checked").val():"number"===r&&void 0!==e.validity?!e.validity.badInput&&s.val():"string"==typeof(i=s.val())?i.replace(/\r/g,""):i},check:function(e){e=this.validationTargetFor(this.clean(e));var i,s,r,n=t(e).rules(),a=t.map(n,function(t,e){return e}).length,o=!1,l=this.elementValue(e);for(s in n){r={method:s,parameters:n[s]};try{if("dependency-mismatch"===(i=t.validator.methods[s].call(this,l,e,r.parameters))&&1===a){o=!0;continue}if(o=!1,"pending"===i)return void(this.toHide=this.toHide.not(this.errorsFor(e)));if(!i)return this.formatAndAdd(e,r),!1}catch(t){throw this.settings.debug&&window.console&&console.log("Exception occurred when checking element "+e.id+", check the '"+r.method+"' method.",t),t instanceof TypeError&&(t.message+=".  Exception occurred when checking element "+e.id+", check the '"+r.method+"' method."),t}}if(!o)return this.objectLength(n)&&this.successList.push(e),!0},customDataMessage:function(e,i){return t(e).data("msg"+i.charAt(0).toUpperCase()+i.substring(1).toLowerCase())||t(e).data("msg")},customMessage:function(t,e){var i=this.settings.messages[t];return i&&(i.constructor===String?i:i[e])},findDefined:function(){for(var t=0;t<arguments.length;t++)if(void 0!==arguments[t])return arguments[t]},defaultMessage:function(e,i){return this.findDefined(this.customMessage(e.name,i),this.customDataMessage(e,i),!this.settings.ignoreTitle&&e.title||void 0,t.validator.messages[i],"<strong>Warning: No message defined for "+e.name+"</strong>")},formatAndAdd:function(e,i){var s=this.defaultMessage(e,i.method),r=/\$?\{(\d+)\}/g;"function"==typeof s?s=s.call(this,i.parameters,e):r.test(s)&&(s=t.validator.format(s.replace(r,"{$1}"),i.parameters)),this.errorList.push({message:s,element:e,method:i.method}),this.errorMap[e.name]=s,this.submitted[e.name]=s},addWrapper:function(t){return this.settings.wrapper&&(t=t.add(t.parent(this.settings.wrapper))),t},defaultShowErrors:function(){var t,e,i;for(t=0;this.errorList[t];t++)i=this.errorList[t],this.settings.highlight&&this.settings.highlight.call(this,i.element,this.settings.errorClass,this.settings.validClass),this.showLabel(i.element,i.message);if(this.errorList.length&&(this.toShow=this.toShow.add(this.containers)),this.settings.success)for(t=0;this.successList[t];t++)this.showLabel(this.successList[t]);if(this.settings.unhighlight)for(t=0,e=this.validElements();e[t];t++)this.settings.unhighlight.call(this,e[t],this.settings.errorClass,this.settings.validClass);this.toHide=this.toHide.not(this.toShow),this.hideErrors(),this.addWrapper(this.toShow).show()},validElements:function(){return this.currentElements.not(this.invalidElements())},invalidElements:function(){return t(this.errorList).map(function(){return this.element})},showLabel:function(e,i){var s,r,n,a=this.errorsFor(e),o=this.idOrName(e),l=t(e).attr("aria-describedby");a.length?(a.removeClass(this.settings.validClass).addClass(this.settings.errorClass),a.html(i)):(s=a=t("<"+this.settings.errorElement+">").attr("id",o+"-error").addClass(this.settings.errorClass).html(i||""),this.settings.wrapper&&(s=a.hide().show().wrap("<"+this.settings.wrapper+"/>").parent()),this.labelContainer.length?this.labelContainer.append(s):this.settings.errorPlacement?this.settings.errorPlacement(s,t(e)):s.insertAfter(e),a.is("label")?a.attr("for",o):0===a.parents("label[for='"+o+"']").length&&(n=a.attr("id").replace(/(:|\.|\[|\]|\$)/g,"\\$1"),l?l.match(new RegExp("\\b"+n+"\\b"))||(l+=" "+n):l=n,t(e).attr("aria-describedby",l),(r=this.groups[e.name])&&t.each(this.groups,function(e,i){i===r&&t("[name='"+e+"']",this.currentForm).attr("aria-describedby",a.attr("id"))}))),!i&&this.settings.success&&(a.text(""),"string"==typeof this.settings.success?a.addClass(this.settings.success):this.settings.success(a,e)),this.toShow=this.toShow.add(a)},errorsFor:function(e){var i=this.idOrName(e),s=t(e).attr("aria-describedby"),r="label[for='"+i+"'], label[for='"+i+"'] *";return s&&(r=r+", #"+s.replace(/\s+/g,", #")),this.errors().filter(r)},idOrName:function(t){return this.groups[t.name]||(this.checkable(t)?t.name:t.id||t.name)},validationTargetFor:function(e){return this.checkable(e)&&(e=this.findByName(e.name)),t(e).not(this.settings.ignore)[0]},checkable:function(t){return/radio|checkbox/i.test(t.type)},findByName:function(e){return t(this.currentForm).find("[name='"+e+"']")},getLength:function(e,i){switch(i.nodeName.toLowerCase()){case"select":return t("option:selected",i).length;case"input":if(this.checkable(i))return this.findByName(i.name).filter(":checked").length}return e.length},depend:function(t,e){return!this.dependTypes[typeof t]||this.dependTypes[typeof t](t,e)},dependTypes:{boolean:function(t){return t},string:function(e,i){return!!t(e,i.form).length},function:function(t,e){return t(e)}},optional:function(e){var i=this.elementValue(e);return!t.validator.methods.required.call(this,i,e)&&"dependency-mismatch"},startRequest:function(t){this.pending[t.name]||(this.pendingRequest++,this.pending[t.name]=!0)},stopRequest:function(e,i){this.pendingRequest--,this.pendingRequest<0&&(this.pendingRequest=0),delete this.pending[e.name],i&&0===this.pendingRequest&&this.formSubmitted&&this.form()?(t(this.currentForm).submit(),this.formSubmitted=!1):!i&&0===this.pendingRequest&&this.formSubmitted&&(t(this.currentForm).triggerHandler("invalid-form",[this]),this.formSubmitted=!1)},previousValue:function(e){return t.data(e,"previousValue")||t.data(e,"previousValue",{old:null,valid:!0,message:this.defaultMessage(e,"remote")})},destroy:function(){this.resetForm(),t(this.currentForm).off(".validate").removeData("validator")}},classRuleSettings:{required:{required:!0},email:{email:!0},url:{url:!0},date:{date:!0},dateISO:{dateISO:!0},number:{number:!0},digits:{digits:!0},creditcard:{creditcard:!0}},addClassRules:function(e,i){e.constructor===String?this.classRuleSettings[e]=i:t.extend(this.classRuleSettings,e)},classRules:function(e){var i={},s=t(e).attr("class");return s&&t.each(s.split(" "),function(){this in t.validator.classRuleSettings&&t.extend(i,t.validator.classRuleSettings[this])}),i},normalizeAttributeRule:function(t,e,i,s){/min|max/.test(i)&&(null===e||/number|range|text/.test(e))&&(s=Number(s),isNaN(s)&&(s=void 0)),s||0===s?t[i]=s:e===i&&"range"!==e&&(t[i]=!0)},attributeRules:function(e){var i,s,r={},n=t(e),a=e.getAttribute("type");for(i in t.validator.methods)"required"===i?(""===(s=e.getAttribute(i))&&(s=!0),s=!!s):s=n.attr(i),this.normalizeAttributeRule(r,a,i,s);return r.maxlength&&/-1|2147483647|524288/.test(r.maxlength)&&delete r.maxlength,r},dataRules:function(e){var i,s,r={},n=t(e),a=e.getAttribute("type");for(i in t.validator.methods)s=n.data("rule"+i.charAt(0).toUpperCase()+i.substring(1).toLowerCase()),this.normalizeAttributeRule(r,a,i,s);return r},staticRules:function(e){var i={},s=t.data(e.form,"validator");return s.settings.rules&&(i=t.validator.normalizeRule(s.settings.rules[e.name])||{}),i},normalizeRules:function(e,i){return t.each(e,function(s,r){if(!1!==r){if(r.param||r.depends){var n=!0;switch(typeof r.depends){case"string":n=!!t(r.depends,i.form).length;break;case"function":n=r.depends.call(i,i)}n?e[s]=void 0===r.param||r.param:delete e[s]}}else delete e[s]}),t.each(e,function(s,r){e[s]=t.isFunction(r)?r(i):r}),t.each(["minlength","maxlength"],function(){e[this]&&(e[this]=Number(e[this]))}),t.each(["rangelength","range"],function(){var i;e[this]&&(t.isArray(e[this])?e[this]=[Number(e[this][0]),Number(e[this][1])]:"string"==typeof e[this]&&(i=e[this].replace(/[\[\]]/g,"").split(/[\s,]+/),e[this]=[Number(i[0]),Number(i[1])]))}),t.validator.autoCreateRanges&&(null!=e.min&&null!=e.max&&(e.range=[e.min,e.max],delete e.min,delete e.max),null!=e.minlength&&null!=e.maxlength&&(e.rangelength=[e.minlength,e.maxlength],delete e.minlength,delete e.maxlength)),e},normalizeRule:function(e){if("string"==typeof e){var i={};t.each(e.split(/\s/),function(){i[this]=!0}),e=i}return e},addMethod:function(e,i,s){t.validator.methods[e]=i,t.validator.messages[e]=void 0!==s?s:t.validator.messages[e],i.length<3&&t.validator.addClassRules(e,t.validator.normalizeRule(e))},methods:{required:function(e,i,s){if(!this.depend(s,i))return"dependency-mismatch";if("select"===i.nodeName.toLowerCase()){var r=t(i).val();return r&&r.length>0}return this.checkable(i)?this.getLength(e,i)>0:e.length>0},email:function(t,e){return this.optional(e)||/^[a-zA-Z0-9.!#$%&'*+\/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/.test(t)},url:function(t,e){return this.optional(e)||/^(?:(?:(?:https?|ftp):)?\/\/)(?:\S+(?::\S*)?@)?(?:(?!(?:10|127)(?:\.\d{1,3}){3})(?!(?:169\.254|192\.168)(?:\.\d{1,3}){2})(?!172\.(?:1[6-9]|2\d|3[0-1])(?:\.\d{1,3}){2})(?:[1-9]\d?|1\d\d|2[01]\d|22[0-3])(?:\.(?:1?\d{1,2}|2[0-4]\d|25[0-5])){2}(?:\.(?:[1-9]\d?|1\d\d|2[0-4]\d|25[0-4]))|(?:(?:[a-z\u00a1-\uffff0-9]-*)*[a-z\u00a1-\uffff0-9]+)(?:\.(?:[a-z\u00a1-\uffff0-9]-*)*[a-z\u00a1-\uffff0-9]+)*(?:\.(?:[a-z\u00a1-\uffff]{2,})).?)(?::\d{2,5})?(?:[\/?#]\S*)?$/i.test(t)},date:function(t,e){return this.optional(e)||!/Invalid|NaN/.test(new Date(t).toString())},dateISO:function(t,e){return this.optional(e)||/^\d{4}[\/\-](0?[1-9]|1[012])[\/\-](0?[1-9]|[12][0-9]|3[01])$/.test(t)},number:function(t,e){return this.optional(e)||/^(?:-?\d+|-?\d{1,3}(?:,\d{3})+)?(?:\.\d+)?$/.test(t)},digits:function(t,e){return this.optional(e)||/^\d+$/.test(t)},creditcard:function(t,e){if(this.optional(e))return"dependency-mismatch";if(/[^0-9 \-]+/.test(t))return!1;var i,s,r=0,n=0,a=!1;if((t=t.replace(/\D/g,"")).length<13||t.length>19)return!1;for(i=t.length-1;i>=0;i--)s=t.charAt(i),n=parseInt(s,10),a&&(n*=2)>9&&(n-=9),r+=n,a=!a;return r%10==0},minlength:function(e,i,s){var r=t.isArray(e)?e.length:this.getLength(e,i);return this.optional(i)||r>=s},maxlength:function(e,i,s){var r=t.isArray(e)?e.length:this.getLength(e,i);return this.optional(i)||s>=r},rangelength:function(e,i,s){var r=t.isArray(e)?e.length:this.getLength(e,i);return this.optional(i)||r>=s[0]&&r<=s[1]},min:function(t,e,i){return this.optional(e)||t>=i},max:function(t,e,i){return this.optional(e)||i>=t},range:function(t,e,i){return this.optional(e)||t>=i[0]&&t<=i[1]},equalTo:function(e,i,s){var r=t(s);return this.settings.onfocusout&&r.off(".validate-equalTo").on("blur.validate-equalTo",function(){t(i).valid()}),e===r.val()},remote:function(e,i,s){if(this.optional(i))return"dependency-mismatch";var r,n,a=this.previousValue(i);return this.settings.messages[i.name]||(this.settings.messages[i.name]={}),a.originalMessage=this.settings.messages[i.name].remote,this.settings.messages[i.name].remote=a.message,s="string"==typeof s&&{url:s}||s,a.old===e?a.valid:(a.old=e,r=this,this.startRequest(i),(n={})[i.name]=e,t.ajax(t.extend(!0,{mode:"abort",port:"validate"+i.name,dataType:"json",data:n,context:r.currentForm,success:function(s){var n,o,l,h=!0===s||"true"===s;r.settings.messages[i.name].remote=a.originalMessage,h?(l=r.formSubmitted,r.prepareElement(i),r.formSubmitted=l,r.successList.push(i),delete r.invalid[i.name],r.showErrors()):(n={},o=s||r.defaultMessage(i,"remote"),n[i.name]=a.message=t.isFunction(o)?o(e):o,r.invalid[i.name]=!0,r.showErrors(n)),a.valid=h,r.stopRequest(i,h)}},s)),"pending")}}});var e,i={};t.ajaxPrefilter?t.ajaxPrefilter(function(t,e,s){var r=t.port;"abort"===t.mode&&(i[r]&&i[r].abort(),i[r]=s)}):(e=t.ajax,t.ajax=function(s){var r=("mode"in s?s:t.ajaxSettings).mode,n=("port"in s?s:t.ajaxSettings).port;return"abort"===r?(i[n]&&i[n].abort(),i[n]=e.apply(this,arguments),i[n]):e.apply(this,arguments)})});
var AjaxCheckout = {
	loadWaiting: false,
	usepopupnotifications: false,
	shippingtotalselector: '.shipping-total',
	totalselector: '.order-total',

	init: function (usepopupnotifications) {
		this.loadWaiting = false;
		this.usepopupnotifications = usepopupnotifications;
	},

	setLoadWaiting: function (display) {
		displayAjaxLoading(display);
		this.loadWaiting = display;
	},

	getdistrictsbyprovinceguid: function (select, districtElementName) {
		if (this.loadWaiting != false) {
			return;
		}
		this.setLoadWaiting(true);

		var urladd = siteRoot + "/Product/Services/CheckoutService.aspx";
		var data = {
			'method': 'GetDistrictsByProvinceGuid',
			'provinceGuid': $(select).val()
		};
		$.ajax({
			cache: false,
			url: urladd,
			data: data,
			type: 'post',
			success: function (result) {
				var district = $("select[name='" + districtElementName + "']");
				$(district).empty();
				$.each(result, function (index, value) {
					$(district).append($("<option></option>").val(value.guid).html(value.name));
				});
			},
			complete: this.resetLoadWaiting,
			error: this.ajaxFailure
		});
	},

	getpaymenttotal: function (radio) {
		var urladd = siteRoot + "/Product/Services/CheckoutService.aspx";

		var data = {
			'method': 'GetPaymentTotal',
			'PaymentMethod': $('input[name=PaymentMethod]:checked').val(),
			'shippingMethodId': $('input[name=ShippingMethod]:checked').val(),
			'SubPaymentMethod' : $(radio).val(),
		};
		$.ajax({
			cache: false,
			url: urladd,
			data: data,
			type: 'post',
			success: function (response) {
				if (response.success == true) {
					$.fancybox.open('<div class="popup-payment-amount">'+response.htmlpopup+'</div>');
				}
			}
		});
	},
	getshippingtotal: function (radio) {
		if ($(radio).is(':checked')) {
			var urladd = siteRoot + "/Product/Services/CheckoutService.aspx";
			var data = {
				'method': 'GetShippingTotal',
				'shippingMethodId': $(radio).val()
			};
			$.ajax({
				cache: false,
				url: urladd,
				data: data,
				type: 'post',
				success: function (response) {
					if (response.success == true) {
						if (AjaxCheckout.shippingtotalselector)
							$(AjaxCheckout.shippingtotalselector).html(response.shippingtotalsectionhtml);
						if (AjaxCheckout.totalselector)
							$(AjaxCheckout.totalselector).html(response.totalsectionhtml);
					}
				}
			});
		}
	},

	saveorder: function (savetodb, redirect) {
		if (this.loadWaiting != false) {
			return;
		}
		this.setLoadWaiting(true);
		var currCulture = "en"
		var lang = {};
		lang['en'] = {
			'Address_FirstName_Required': 'Please Enter Your First Name',
			'Address_LastName_Required': 'Please Enter Your Last Name',
			'Address_Email_Required': 'Email Not Valid',
			'Address_Address_Required': 'Please Enter Your Address',
			'Address_Phone_Required': 'Please Enter Your Phone',
			'Address_Phone_NotValid': 'Your Phone  Not Valid',
			'Address_Phone_minlength': 'Please enter the phone from 8-10 numbers',
			'Address_Province_Required': 'Please Enter Your Province',
			'Address_District_Required': 'Please Enter Your District',
			'Address_ShipingMethod_Required': 'Please Enter choose Shipping Method',
			'Address_PaymentMethod_Required': 'Please choose Payment Method'
		};
		lang['vi'] = {
			'Address_FirstName_Required': 'Vui lòng nhập Tên',
			'Address_LastName_Required': 'Vui lòng nhập Họ',
			'Address_Email_Required': 'Email không hợp lệ',
			'Address_Address_Required': 'Vui lòng nhập Địa chỉ',
			'Address_Phone_Required': 'Vui lòng nhập Số điện thoại',
			'Address_Phone_NotValid': 'Điện thoại không hợp lệ',
			'Address_Phone_minlength': 'Vui lòng nhập điện thoại từ 8-10 số',
			'Address_Province_Required': 'Vui lòng nhập Tỉnh/Thành',
			'Address_District_Required': 'Vui lòng nhập Quận/Huyện',
			'Address_ShipingMethod_Required': 'Please Enter Your District',
			'Address_PaymentMethod_Required': 'Vui lòng chọn phương thức thanh toán'
		};
		if ($("body").hasClass("vi-vn"))
			currCulture = "vi";
		$("#aspnetForm").validate({
			lang: currCulture,
			onsubmit: false,
			rules: {
				Address_FirstName: "required",
				Address_LastName: "required",
				Address_Email: {
					email: true
				},
				Address_Address: "required",
				Address_Phone: {
					required: true,
					number: true,
					minlength: 8,
					maxlength: 10
				},
				Address_Province: "required",
				Address_District: "required",
				ShippingMethod: "required",
				PaymentMethod: "required",
				Invoice_CompanyTaxCode: {
					required: "#Invoice_Required:checked"
				},
				Invoice_CompanyName: {
					required: "#Invoice_Required:checked"
				},
				Invoice_CompanyAddress: {
					required: "#Invoice_Required:checked"
				}
			},
			messages: {
				Address_FirstName: lang[currCulture].Address_FirstName_Required,
				Address_LastName: lang[currCulture].Address_LastName_Required,
				Address_Email: lang[currCulture].Address_Email_Required,
				Address_Address: lang[currCulture].Address_Address_Required,
				Address_Phone: {
					required: lang[currCulture].Address_Phone_Required,
					number: lang[currCulture].Address_Phone_NotValid,
					minlength: lang[currCulture].Address_Phone_minlength
				},
				Address_Province: lang[currCulture].Address_Province_Required,
				Address_District: lang[currCulture].Address_District_Required,
				ShippingMethod: lang[currCulture].Address_ShipingMethod_Required,
				PaymentMethod: lang[currCulture].Address_PaymentMethod_Required,
				Invoice_CompanyTaxCode: "Vui lòng nhập Mã số thuế",
				Invoice_CompanyName: "Vui lòng nhập Tên công ty",
				Invoice_CompanyAddress: "Vui lòng nhập Địa chỉ công ty"
			}
		});

		if (!$("#aspnetForm").valid()) {
			this.setLoadWaiting(false);
			return;
		}
		if($('input[name=PaymentMethod]:checked').val() == undefined)
		{
			this.setLoadWaiting(false);
			alert(lang[currCulture].Address_PaymentMethod_Required);
			return;
		}else if($('input[name=PaymentMethod]:checked').parents(".method").find(".sub-method").length > 0)
		{
			if($('input[name=PaymentMethod]:checked').parents(".method").find(".sub-method input[type=radio]:checked").length == 0)
			{
				this.setLoadWaiting(false);
				alert(lang[currCulture].Address_PaymentMethod_Required);
				return;
			}
		}



		var urladd = siteRoot + "/Product/Services/CheckoutService.aspx";
		var data = $('#aspnetForm').serializeArray();
		data.push({
			name: 'method',
			value: 'SaveOrder'
		});
		data.push({
			name: 'savetodb',
			value: savetodb
		});
		data.push({
			name: 'redirect',
			value: redirect
		});

		//        $("input[type=text][required],select[required]").each(function(){
		//            data.push({ name: $(this).attr("name") + ".Required", value: true });
		//        });

		$.ajax({
			cache: false,
			url: urladd,
			data: data,
			type: 'post',
			success: function (response) {
				if (response.success == true) {
					if (response.redirect) {
						setLocation(response.redirect);
					}
				} else {
					//error
					displayBarNotification(response.message, 'error', 0);
				}
			},
			complete: this.resetLoadWaiting,
			error: this.ajaxFailure
		});
	},
	resetLoadWaiting: function () {
		AjaxCheckout.setLoadWaiting(false);
	},

	ajaxFailure: function () {
		alert('Failed to process checkout. Please refresh the page and try one more time.');
	}
}

/*************************************************************************************************************/
/* END CHECKOUT
/*************************************************************************************************************/