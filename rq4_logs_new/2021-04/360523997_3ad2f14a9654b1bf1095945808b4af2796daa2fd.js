jQuery(document).ready(function(){

	/* Ajax adding to cart when user click on add to cart button */
	jQuery(document).on('click', '.btn-cart', function(){
		var product = jQuery(this).data('product');
		var url = jQuery(this).data('url');
		var condition = jQuery(this).data('condition');
		var qty;

		if( jQuery('input').is('#qty') ){
			qty = jQuery('#qty').val();
		} else {
			qty = 1;
		}
		url = url.replace("checkout/cart","ajax/index");
		var data = 'product='+product+'&related_product=&qty='+qty+'&isAjax=1';

		/* Checking if it used product and show terms */
		if( condition == 'used' ) {
			showTerms(url,data);
			return;
		}
		
		showLoader(true);
		addToCart(product,url,condition,data);
	})

	/* Add to cart used product if user click accept button in term pop-up */
	jQuery(document).on('click', '.used-add-to-cart', function(){
		var url = jQuery(this).data('url');
		var data = jQuery(this).data('query');

		closeTerms();
		showLoader(true);
		addToCart(null,url,'used',data);
		
	})

	/* Close terms pop-up window if user click cancel button */
	jQuery(document).on('click', '.used-cancel, .used-agree', function(){
		closeTerms();
	})

	/* Check the checkbox by clicking on product in a pop-up crosssell */
	jQuery(document).on('click', '.items', function(event){

		var product = jQuery(this).data('product');
		var checkbox = jQuery('#checkbox_product_'+product);

		if (event.target.type !== 'checkbox') {
			checkbox.trigger('click');
		}

		selectedProducts();
	})

	/* Add to cart products from pop-up crosssell */
	jQuery(document).on('click', '.modal-add-to-cart', function(){
		var counter = 0;
		var selectedLength = 0;
		var selected = new Object();

		/* Get all checked products */
		jQuery('.main_container input:checked').each(function() {
			var product = jQuery(this).data('product');
			var url = jQuery(this).data('url');
			selected[product] = url;
		});

		/* Quantity of checked products  */
		jQuery.each(selected, function(i, val) {
			selectedLength++;
		});

		if( jQuery.isEmptyObject(selected) == true) {
			jQuery('.bottom_selected_products').html('Non hai selezionato nessun prodotto');
			return;
		} else {
			showLoader(true);
			for (var arrayIndex in selected) {

				var url = selected[arrayIndex];
				var product = arrayIndex;
				var data = 'product='+product+'&related_product=&qty=1&isAjax=1';
				var e = null;
				url = url.replace("checkout/cart","ajax/index");

				try {
					jQuery.ajax({
						url: url,
						dataType: 'json',
						type : 'post',
						data: data,
						async: true,
						success: function(data){

							/* If you need to show adding status
							if(data.status == 'SUCCESS'){} else {}
							*/

							changeMiniCart(data.getItemsCount);
							counter++;

						},
						error: function(xhr, status, error) {
							console.log(xhr.responseText);
						}
					})
				} catch (e) {
				}				
			}
			timer = setInterval( function(){
				if(counter == selectedLength){
					showLoader(false);
					goToCart();
					clearInterval(timer);
				}
			},500)
		}
	})

	jQuery(document).on('click', '.modal-go-to-cart', function(){
		goToCart();
	})

	jQuery(document).on('click', '.modal-continue-shopping', function(){
		closeCrosssell();
	})

	jQuery(document).on('click', '#need_invoice', function(){
		if( jQuery(this).prop('checked') === true ) {
			showInvoiceType();
		} else {
			var firstname = jQuery('input[name="billing[firstname]"]');
			var lastname = jQuery('input[name="billing[lastname]"]');
			var company = jQuery('input[name="billing[company]"]');
			
			firstname.addClass('required-entry');
			lastname.addClass('required-entry');
			company.removeClass('required-entry');
			company.removeClass('validation-failed');

			company.parent().find('.validation-advice').fadeOut(500);
			company.parent().parent().find('label').find('em').remove();
			company.parent().parent().find('label').removeClass('required');

			firstname.parent().parent().find('label').find('em').show();
			lastname.parent().parent().find('label').find('em').show();


			if(!firstname.val()) {
				firstname.removeClass('validation-passed');
				firstname.addClass('validation-failed');
			}

			if(!lastname.val()) {
				lastname.removeClass('validation-passed');
				lastname.addClass('validation-failed');
			}		
		}
	})

	jQuery(document).on('click', '.checkout-accept-invoice-type', function(){
		acceptInvoiceType();
	})

})

function goToCart(){
	setLocation('/checkout/cart/');
}

/* Make pop-up window with additional products */
function showCrosssell(html){
	jQuery("<div/>", {
		"class": "modal",
	}).appendTo("body");
	jQuery("<div/>", {
		"class": "crosssell_product_modal",
	}).appendTo(".modal");
	jQuery('body').addClass('modal-open')
	jQuery('.crosssell_product_modal').html(html);
	jQuery('.crosssell_product_modal').fadeIn();
}

/* Delete pop-up window with additional products */
function closeCrosssell(){
	jQuery('body').removeClass('modal-open')
	jQuery('.modal').fadeOut(function(){
		jQuery('.modal').remove();
	});
}

/* Show terms */
function showTerms(url,query){
	var addButtons = '<span class="abutton used-add-to-cart" data-url="'+url+'" data-query="'+query+'">Accetto</span><span class="abutton link-compare used-cancel">Non accetto</span>';
	jQuery("<div/>", {
		"class": "used-terms",
	}).appendTo("body");
	jQuery('.used-terms').html('<h2>GARANZIA RIGENERATI</h2>'+
		'Ai sensi dell\'art. 134 del D.Lgs 205/2006, le parti pattuiscono che la garanzia per gli articoli rigenerati avrà durata di un anno a decorrere dalla consegna.'+
		'<div class="used-bottom">'+addButtons+'</div>');
	jQuery('.used-terms').fadeIn();	
}

function closeTerms(){
	jQuery('.used-terms').fadeOut(function(){
		jQuery('.used-terms').remove();
	});	
}

function showLoader(param){
	var loader = '<div class="sk-circle"><div class="sk-circle1 sk-child"></div><div class="sk-circle2 sk-child"></div><div class="sk-circle3 sk-child"></div><div class="sk-circle4 sk-child"></div><div class="sk-circle5 sk-child"></div><div class="sk-circle6 sk-child"></div><div class="sk-circle7 sk-child"></div><div class="sk-circle8 sk-child"></div><div class="sk-circle9 sk-child"></div><div class="sk-circle10 sk-child"></div><div class="sk-circle11 sk-child"></div><div class="sk-circle12 sk-child"></div></div>';
	if( !jQuery('div').is('#ajax_loader') ){
		jQuery("<div/>", {
			"id": "ajax_loader",
		}).appendTo("body");
		jQuery('#ajax_loader').html(loader);		
	}

	if( param == true ){
		jQuery('#ajax_loader').show();
	} else {
		jQuery('#ajax_loader').hide();
	}
}

function addToCart(product,url,condition,data){
	try {
		jQuery.ajax({
			url: url,
			dataType: 'json',
			type : 'post',
			data: data,
			async: true,
			success: function(data){
				showLoader(false);

				//if(data.status == 'SUCCESS'){} else {}

				changeMiniCart(data.getItemsCount);

				if(condition == 'new' && data.crosssell){
					showCrosssell(data.crosssell);
				}
				if(condition == 'used'){
					goToCart();
				}
				return;
			},
			beforeSend: function(){
			},
			error: function(xhr, status, error) {
				console.log(xhr.responseText);
			}
		})
	} catch (e) {
	}
}

function selectedProducts(){
	var selected = []
	jQuery('.main_container input:checked').each(function() {
		var product = jQuery(this).data('product');
		selected.push(product);
	});

	if(selected.length == 0) {
		jQuery('.bottom_selected_products').html('');
	} else {
		jQuery('.bottom_selected_products').html('Hai selezionato <strong>'+selected.length+'</strong> prodott'+(selected.length == 1 ? 'o' : 'i') );
	}
}

/* Update information in header mini-cart */
function changeMiniCart(qty){
	if(qty){
		if(qty == 1){
			jQuery('a.amount').html('('+qty+') Prodotto inserito');
		} else if (qty > 1){
			jQuery('a.amount').html('('+qty+') Prodotti inseriti');
		} else {
			jQuery('a.amount').html('(0) Nessun prodotto inserito');
		}
	}
}

/* Show question for invoice */
function showInvoiceType(){
	var addButtons = '<span class="abutton checkout-accept-invoice-type">Accetto</span>';

	jQuery("<div/>", {
		"class": "checkout-invoice-type",
	}).appendTo("body");
	jQuery('.checkout-invoice-type').html('<h2>Ho bisogno della fattura per una</h2>'+
		'<label><input type="radio" name="invoice-type" value="nonbusiness"> Privato</label><br><br>'+
		'<label><input type="radio" name="invoice-type" value="business"> Persona fisica titolare di una ditta individuale/Libero professionista/Società</label><br>'+
		'<div class="checkout-invoice-bottom">'+addButtons+'</div>'
	);
	jQuery('.checkout-invoice-type').fadeIn();	
}

function acceptInvoiceType(){
	var type = jQuery('input[name=invoice-type]:checked').val();
		
	var firstname = jQuery('input[name="billing[firstname]"]');
	var lastname = jQuery('input[name="billing[lastname]"]');
	var company = jQuery('input[name="billing[company]"]');

	var vat = jQuery('input[name="billing[vat_id]"]')
	var fiscalcode = jQuery('input[name="billing[fiscal_code]"]')

	var companycode = jQuery('input[name="billing[company_code]"]')

	var pec = jQuery('input[name="billing[pec]"]')

	if( typeof(type) == 'undefined' ) {
		alert('Please, make a choise');
		return;
	} else if(type == 'business') {
		firstname.removeClass('required-entry');
		lastname.removeClass('required-entry');
		company.addClass('required-entry');

		companycode.parent().parent().removeClass('hidden')
		companycode.addClass('required-entry');
		pec.parent().parent().removeClass('hidden')
		pec.addClass('required-entry');

		firstname.parent().find('.validation-advice').fadeOut(500);
		lastname.parent().find('.validation-advice').fadeOut(500);

		company.parent().parent().find('label').append('<em>*</em>');
		company.parent().parent().find('label').addClass('required');

		firstname.parent().parent().find('label').find('em').hide();
		lastname.parent().parent().find('label').find('em').hide();


		firstname.removeClass('validation-failed');
		lastname.removeClass('validation-failed');


		if(!company.val()) {
			company.removeClass('validation-passed');
			company.addClass('validation-failed');
		}

		fiscalcode.parent().parent().removeClass('hidden')
		fiscalcode.addClass('required-entry');
		Validation.validate($('billing:fiscal_code'))

	} else if(type == 'nonbusiness') {
		firstname.addClass('required-entry');
		lastname.addClass('required-entry');
		company.removeClass('required-entry');
		company.removeClass('validation-failed');

		vat.removeClass('required-entry');
		vat.removeClass('validation-failed');
		jQuery('.billing-vat-field').addClass('hidden')

		companycode.removeClass('required-entry');
		pec.removeClass('required-entry');

		companycode.val('0000000');
		
		Validation.validate($('billing:company_code'));
		companycode.removeClass('validation-failed');
		pec.removeClass('validation-failed');

		company.parent().find('.validation-advice').fadeOut(500);

		if(!firstname.val()) {
			firstname.removeClass('validation-passed');
			firstname.addClass('validation-failed');
		}

		if(!lastname.val()) {
			lastname.removeClass('validation-passed');
			lastname.addClass('validation-failed');
		}

		fiscalcode.parent().parent().removeClass('hidden')
		fiscalcode.addClass('required-entry');
		Validation.validate($('billing:fiscal_code'))
		
	}

	jQuery('.checkout-invoice-type').remove();
}

/*
 * jquery-gpopover
 * http://github.com/markembling/jquery-gpopover
 *
 * A simple jQuery plugin for creating popover elements similar to Google's 
 * new 'apps' launcher/switcher.
 *
 * Copyright (c) 2013 Mark Embling (markembling.info)
 * Licensed under the BSD (3 clause) license.
 */

;(function($){
    
    var GPopover = function(element, options) {
        this.options = null;
        this.$trigger = null;
        this.$popover = null;
        
        this.init(element, options);
    }
    
    GPopover.prototype.init = function(element, options) {
        var that = this;
        
        this.options = $.extend({}, $.fn.gpopover.defaults, options);
        
        this.$trigger = $(element);
        this.$popover = $('#' + this.$trigger.data('popover'));
        
        this._addArrowElements();
        
        if (this.options.preventHide) {
            this._preventHideClickPropagation();
        }
        
        this.$trigger.click(function(e){
            if (! that.$popover.is(":visible")) {
                // Trigger a click on the parent element (that can bubble up)
                $(this).parent().click();
                
                that.show();
                
                e.stopPropagation();
            }
            
            e.preventDefault();
        });
    }
    
    GPopover.prototype.show = function() {
        var that = this;
        
        // Set width before showing
        this.$popover.width(this.options.width);
        
        // Show the popover
        this.$popover.fadeIn(this.options.fadeInDuration);
        
        // Set up hiding
        $(document).one('click.popoverHide', function() {
            // _hidePopover($popover, settings);
            that.hide();
        });
    
        // Sort out the position (must be done after showing)
        var triggerPos = this.$trigger.offset();
        this.$popover.offset({
            left: (triggerPos.left + (this.$trigger.outerWidth() / 2)) - (this.$popover.outerWidth() / 2),
            top: triggerPos.top + this.$trigger.outerHeight() + 10  
            // the final 10 above allows room for the arrow above it
        });
    
        // Check and reposition if out of the viewport
        var positionXCorrection = this._repositionForViewportSides();
    
        // Set the position of the arrow elements
        this._setArrowPosition(positionXCorrection);
        
        // Call the callback
        this.options.onShow.call(this.$trigger, this.$popover);
    }
    
    GPopover.prototype.hide = function() {
        // Hide the popover
        this.$popover.fadeOut(this.options.fadeOutDuration);
        
        // Call the callback
        this.options.onHide.call(this.$trigger, this.$popover);
    }
    
    GPopover.prototype._addArrowElements = function() {
        this.$arrow = $('<div class="gpopover-arrow"></div>');
        this.$arrowShadow = $('<div class="gpopover-arrow-shadow"></div>');
        
        this.$popover.append(this.$arrow);
        this.$popover.append(this.$arrowShadow);
    }
    
    GPopover.prototype._preventHideClickPropagation = function() {
        /* Prevent clicks within the popover from being propagated 
           to the document (and thus stop the popover from being 
           hidden) */
        this.$popover.click(function(e) { e.stopPropagation(); });
    }
    
    GPopover.prototype._repositionForViewportSides = function() {
        var popoverOffsetLeft = this.$popover.offset().left,
            positionXCorrection = 0,
            $window = $(window);
        
        // Right edge
        if (popoverOffsetLeft + this.$popover.outerWidth() + this.options.viewportSideMargin > $window.width()) {
            var rightEdgeCorrection = -((popoverOffsetLeft + this.$popover.outerWidth() + this.options.viewportSideMargin) - $window.width());
            popoverOffsetLeft = popoverOffsetLeft + rightEdgeCorrection
        
            positionXCorrection = rightEdgeCorrection;
        }
        
        // Left edge
        if (popoverOffsetLeft < this.options.viewportSideMargin) {
            var leftEdgeCorrection = this.options.viewportSideMargin - popoverOffsetLeft;
            popoverOffsetLeft = popoverOffsetLeft + leftEdgeCorrection
            
            positionXCorrection += leftEdgeCorrection;
        }
        
        // Reposition the popover element if necessary
        if (positionXCorrection !== 0) {
            this.$popover.offset({ left: popoverOffsetLeft });
        }
        
        return positionXCorrection;
    }
    
    GPopover.prototype._setArrowPosition = function(positionXCorrection) {
        var leftPosition = (this.$popover.outerWidth() / 2) - (this.$arrow.outerWidth() / 2) - positionXCorrection;
        
        this.$arrow.css({ top: -7, left: leftPosition });
        this.$arrowShadow.css({ top: -8, left: leftPosition });
    }
    
    $.fn.gpopover = function(option) {
        return this.each(function(){
            var $this = $(this),
                data = $this.data('gpopover'),
                options = (typeof option == 'object' && option);
                
            // Initialise if not already done
            if (!data) {
                data = new GPopover(this, options);
                $this.data('gpopover', data);
            }
            
            // If the option parameter was a string, trigger the named function
            if (typeof option == 'string') data[option]();
        });
        
    };
    
    // Default settings
    $.fn.gpopover.defaults = {
        width: 250,             // Width of the popover
        fadeInDuration: 65,     // Duration of popover fade-in animation
        fadeOutDuration: 65,    // Duration of popover fade-out animation
        viewportSideMargin: 10, // Space to leave the side if out the viewport
        preventHide: false,     // Prevent hide when clicking within popover
        
        onShow: function() {},  // Called upon showing the popover
        onHide: function() {}   // Called upon hiding the popover
    };
    
})(jQuery);