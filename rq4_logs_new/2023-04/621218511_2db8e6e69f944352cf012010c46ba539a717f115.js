Vtiger_Edit_Js("JobCard_Edit_Js", {}, {
    registerBasicEvents: function (container) {
		this._super(container);
        this.registerReferenceSelectionEvent(container);
	
	},
    registerReferenceSelectionEvent: function (container) {
		var self = this;
		jQuery('input[name="product_name"]', container).on(Vtiger_Edit_Js.referenceSelectionEvent, function (e, data) {
			self.autofill();
		});

    },
    autofill: function () {
		var self = this;
		let dataOf = {};
		dataOf['record'] =$("input[name=product_name]").val();
		dataOf['source_module'] = 'JobCard';
		dataOf['module'] = 'JobCard';
		dataOf['action'] = 'GetAllInfo';
		app.helper.showProgress();
		app.request.get({ data: dataOf }).then(function (err, response) {
			console.log(response);
			if (err == null) {
				let referenceFields = { 'product_subcategory':'product_subategory' ,'productcategory':"product_category", 'productcode': 'serial_number','purchase_cost':'final_amount','purchased_date':"" };
				for (var key in referenceFields) {
					let value = referenceFields[key];
					self.seletctTheMarkVendor(response['data'][key], response['data'][key + '_label'], value);
				}
				var modalName = response.data.product_subcategory;
				console.log(modalName)
				$('.product_subategory  option[value="' + modalName + '"]').attr("selected", "selected").trigger('change');
				$('[data-fieldname="product_subategory"]').attr("readonly", 'readonly');

				modalName = response.data.productcategory;
				console.log(modalName)
				$('.product_category  option[value="' + modalName + '"]').attr("selected", "selected").trigger('change');
				$('[data-fieldname="product_category"]').attr("readonly", 'readonly');
				app.helper.hideProgress();
			}
		});
	},


    seletctTheMarkVendor: function (id, label, field) {
		let selectedNameOfAsset = label;
		let sourceField = field;
		var fieldElement = jQuery("#" + app.getModuleName() + "_editView_fieldName_" + field + "_select");
		var sourceFieldDisplay = field + "_display";
		var fieldDisplayElement = jQuery('input[name="' + sourceFieldDisplay + '"]');
		var popupReferenceModuleElement = jQuery('input[name="popupReferenceModule"]').length ? jQuery('input[name="popupReferenceModule"]') : jQuery('input.popupReferenceModule');
		var popupReferenceModule = popupReferenceModuleElement.val();
		var selectedName = selectedNameOfAsset;
		jQuery('input[name="' + sourceField + '"]').val(id);
		if (id && selectedName) {
			if (!fieldDisplayElement.length) {
				fieldElement.attr('value', id);
				fieldElement.data('value', id);
				fieldElement.val(selectedName);
			} else {
				fieldElement.val(id);
				fieldElement.data('value', id);
				fieldDisplayElement.val(selectedName);
				if (selectedName) {
					fieldDisplayElement.attr('readonly', 'readonly');
				} else {
					fieldDisplayElement.removeAttr("readonly");
				}
			}
			if (selectedName) {
				fieldElement.parent().parent().find('#' + sourceFieldDisplay).attr('disabled', 'disabled');
				fieldElement.parent().parent().find('.clearReferenceSelection').removeClass('hide');
				fieldElement.parent().parent().find('.referencefield-wrapper').addClass('selected');
			} else {
				fieldElement.parent().parent().find('.clearReferenceSelection').addClass('hide');
				fieldElement.parent().parent().find('.referencefield-wrapper').removeClass('selected');
			}
			fieldElement.trigger(Vtiger_Edit_Js.referenceSelectionEvent, { 'source_module': popupReferenceModule, 'record': id, 'selectedName': selectedName });
		}
	},
});