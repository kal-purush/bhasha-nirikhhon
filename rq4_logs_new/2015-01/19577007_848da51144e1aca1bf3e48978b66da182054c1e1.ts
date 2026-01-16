
import IEntity = require('../IEntity');
import EntityPreparer = require('../EntityPreparer');
import ValueTypeEnum = require('./ValueTypeEnum');
import LocalizedString = require('../Locale/LocalizedString');
import IValueObject = require('./IValueObject');

export = Value;
class Value implements IEntity {

	private valueTypeNames: {[valueType: number]: LocalizedString};

	get ValueType() { return this.valueType; }
	get DateChecked() { return this.dateChecked; }
	
	constructor(
		private valueType: ValueTypeEnum,
		private dateChecked: Date
	) {
		this.valueTypeNames = {};
		this.valueTypeNames[ValueTypeEnum.EAN] = new LocalizedString({ cs: 'EAN', en: 'EAN' });
		this.valueTypeNames[ValueTypeEnum.DESCRIPTION] = new LocalizedString({ cs: 'Popis', en: 'Description' });
		this.valueTypeNames[ValueTypeEnum.PRICE] = new LocalizedString({ cs: 'Cena', en: 'Price' });
		this.valueTypeNames[ValueTypeEnum.TRAFIC] = new LocalizedString({ cs: 'Návštěvnost', en: 'Trafic' });
		this.valueTypeNames[ValueTypeEnum.MAIN_IMAGE] = new LocalizedString({ cs: 'Obrázek', en: 'Image' });
	}

	getValueTypeName() {
		return this.valueTypeNames[this.valueType];
	}

	isChecked() {
		return this.dateChecked !== null;
	}

	static fromObject(object: IValueObject) {
		return new Value(
			EntityPreparer.enum<ValueTypeEnum>(ValueTypeEnum, object.valueType),
			EntityPreparer.dateOrNull(object.dateChecked)
		);
	}

	static toObject(entity: Value): IValueObject {
		return {
			valueType: ValueTypeEnum[entity.valueType],
			dateChecked: entity.dateChecked
		};
	}

	toObject() {
		return Value.toObject(this);
	}
}