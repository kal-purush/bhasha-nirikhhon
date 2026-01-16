import {WikitextGenerator} from '../WikitextGenerator';
import {WikitextTemplate} from '../WikitextTemplate';
import {Utils} from '../Utils';
import {Promise, hash} from '../vendor/rsvp-latest.js';

export class FurnitureGenerator implements WikitextGenerator {
	private template : WikitextTemplate;
	private findById = (list, id) => list.filter((item) => parseInt(item.furniture_item_id) === id);

	constructor(template) {
		this.template = template;
	}

	render(itemid) {
		var promise = new Promise(function(resolve, reject) {
			let promises = Utils.map(Utils.full_langs, (_, lang) => Utils.getJson('http://cdn.clubpenguin.com/play/' + lang + '/web_service/game_configs/furniture_items.json'));
			hash(promises).then(function(data) {
				let item = this.findById(data.en, itemid);
				if (item.length !== 1) {
					reject('Unknown item id');
				}

				item = item[0];
				item.id = itemid;
				for (let lang in Utils.full_langs) {
					item['label_' + lang] = this.findById(data[lang], itemid)[0].label;
				}
				resolve(this.template.render({item: item}));
			}.bind(this)).catch((msg) => reject(msg));
		}.bind(this));
		return promise;
	}
}