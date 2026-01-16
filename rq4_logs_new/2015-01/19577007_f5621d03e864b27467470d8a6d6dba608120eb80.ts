
import IEntity = require('../IEntity');
import ICheckItemObject = require('../../Definition/Checklist/ICheckItemObject');
import EntityPreparer = require('../EntityPreparer');
import CheckTypeEnum = require('./CheckTypeEnum');

export = CheckItem;
class CheckItem implements IEntity {

	constructor(
		private checkType: CheckTypeEnum,
		private dateChecked: Date
	) {}
	
	get CheckType() { return this.checkType; }
	get DateChecked() { return this.dateChecked; }

	static fromObject(object: ICheckItemObject) {
		return new CheckItem(
			CheckTypeEnum[object.checkType],
			EntityPreparer.dateOrNull(object.dateChecked)
		);
	}

	static toObject(entity: CheckItem): ICheckItemObject {
		return {
			checkType: CheckTypeEnum[entity.checkType],
			dateChecked: entity.dateChecked
		};
	}

	toObject() {
		return CheckItem.toObject(this);
	}
}