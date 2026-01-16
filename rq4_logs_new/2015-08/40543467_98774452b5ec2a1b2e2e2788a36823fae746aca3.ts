/// <reference path="../../typings/tsd.d.ts" />

import m = require('mithril');

class CardForm implements MithrilModule {
	controller() {
	}
	
	view(ctrl) {
		return m('div.container.form-horizontal', [
			m('div.form-group', [
				m('label[for="deck"].control-label.col-xs-2', 'Deck'),
				m('div.col-xs-4', [
					m('select[name="deck"].form-control', [
						m('option[value="foo"]', 'foo'),
						m('option[value="bar"]', 'bar')	
					])						
				])
			]),
			m('hr'),
			m('div.form-group', [
				m('label[for="front"].control-label.col-xs-2', 'Front'),
				m('div.col-xs-10', [
					m('input[type="text", name="front"].form-control')
				])
			]),
			m('div.form-group', [
				m('label[for="back"].control-label.col-xs-2', 'Back'),
				m('div.col-xs-10', [
					m('input[type="text", name="back"].form-control')
				])
			])
		]);
	}
}

export = CardForm;