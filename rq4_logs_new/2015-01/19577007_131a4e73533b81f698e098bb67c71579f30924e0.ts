
import IEntity = require('../IEntity');
import EntityPreparer = require('../EntityPreparer');
import IImageObject = require('../../Definition/Image/IImageObject');

export = Image;
class Image implements IEntity {

	constructor(
		private id: string
	) {}
	
	get Id() { return this.id; }

	static fromObject(object: IImageObject) {
		return new Image(
			EntityPreparer.id(object.id)
		);
	}

	static toObject(entity: Image): IImageObject {
		return {
			id: entity.id
		};
	}

	toObject() {
		return Image.toObject(this);
	}
}