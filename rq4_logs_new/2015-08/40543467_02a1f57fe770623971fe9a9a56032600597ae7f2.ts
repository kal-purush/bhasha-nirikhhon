import m = require('mithril');

import Deck = require('./deck');

var decks = [
	new Deck('Magyar'),
	new Deck('Nihongo')
];

class DeckList implements MithrilModule {
	controller() {
	}
	
	view(ctrl) {
		var rows = decks.map(x => {
			return m('tr', [
				m('td', x.name),
				m('td', 0),
				m('td', 0),
				m('td')
			])	
		});
		
		return m('table.table.table-hover', [
			m('thead', [
				m('tr', [
					m('th', 'Deck'),
					m('th', 'Due'),
					m('th', 'New'),
					m('th')
				])
			]),
			m('tbody', rows)
		]);
	}	
}

export = DeckList;