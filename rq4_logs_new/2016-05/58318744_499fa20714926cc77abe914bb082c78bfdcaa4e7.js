/* eslint-disable no-undef, array-callback-return  */

CKEDITOR.dialog.add('calendarDialog', function (editor) {
	return {
		title: 'Google Calendar Embed Code',
		minWidth: 400,
		minHeight: 200,
		contents: [
			{
				id: 'tab-basic',
				label: 'Embed Settings',
				elements: [{
					type: 'textarea',
					id: 'embedMarkup',
					label: 'Embed Code',
					validate: CKEDITOR.dialog.validate.notEmpty('Embed cannot be empty.'),
					setup: function (element) {
						var containingElement = element.$.firstElementChild;

						if (containingElement && containingElement.localName === 'iframe') {
							this.setValue(containingElement.outerHTML);
						}
					}
				}]
			}
		],
		onShow: function () {
			var selection = editor.getSelection();
			var element = selection.getStartElement();

			this.insertMode = false;

			if (!element || !element.hasClass('calendar-insulator')) {
				element = editor.document.createElement('div');
				this.insertMode = true;
			}

			this.element = element;

			if (this.insertMode === false) {
				this.setupContent(element);
			}
		},
		onOk: function () {
			var dialog = this;
			var element = dialog.element;

			dialog.commitContent(element);

			// the iframe html from embedMarkup
			var embedIframe = CKEDITOR.dom.element.createFromHtml(dialog.getValueOf('tab-basic', 'embedMarkup'));

			if (dialog.insertMode) {
				element.setAttribute('class', 'calendar-insulator');
				embedIframe.appendTo(element);
				editor.insertElement(element);
			}
		}
	};
});