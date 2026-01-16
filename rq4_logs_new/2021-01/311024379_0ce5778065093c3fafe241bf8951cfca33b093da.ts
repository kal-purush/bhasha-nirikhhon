import assert from 'assert';
import { SearchConsistency } from 'ottoman';
import { bird, eagle, falcon, hawk, sparrow, vulture } from './setup/fixtures';
import { getMongooseModel, getOttomanModel } from './setup/model';
import { assertAirline, removeDocuments } from './setup/util';

describe('test create function', async () => {
    it('mongoose - should create new doc', async () => {
        const Airplane = getMongooseModel();

        const stringAirplane = new Airplane(hawk);
        const createdString = await Airplane.create(stringAirplane);
        assertAirline(createdString, stringAirplane);
        assert.ok(typeof createdString.extension === 'string');

        const numberAirplane = new Airplane(eagle);
        const createdNumber = await Airplane.create(numberAirplane);
        assertAirline(createdNumber, numberAirplane);
        assert.ok(typeof createdNumber.extension === 'number');

        const booleanAirplane = new Airplane(falcon);
        const createdBoolean = await Airplane.create(booleanAirplane);
        assertAirline(createdBoolean, booleanAirplane);
        assert.ok(typeof createdBoolean.extension === 'boolean');

        const dateAirplane = new Airplane(sparrow);
        const createdDate = await Airplane.create(dateAirplane);
        assertAirline(createdDate, dateAirplane);
        assert.ok(typeof createdString.extension === 'object');

        const arrayAirplane = new Airplane(vulture);
        const createdArray = await Airplane.create(arrayAirplane);
        assertAirline(createdArray, arrayAirplane);
        assert.ok(typeof createdArray.extension === 'object');
        assert.deepStrictEqual(createdArray.extension, vulture.extension);

        const arrayObjdAirplane = new Airplane(bird);
        const createdArrayObj = await Airplane.create(arrayObjdAirplane);
        assertAirline(createdArrayObj, arrayObjdAirplane);
        assert.ok(typeof createdArrayObj.extension === 'object');
        assert.deepStrictEqual(createdArrayObj.extension, bird.extension);

        const objBird = { ...bird, extension: { external: 'field' } };
        const objectAirplane = new Airplane(objBird);
        const createdObject = await Airplane.create(objectAirplane);
        assertAirline(createdObject, objectAirplane);
        assert.ok(typeof createdObject.extension === 'object');
        assert.deepStrictEqual(createdObject.extension, objBird.extension);

        const find = await Airplane.find().exec();
        assert.strictEqual(find.length, 1);
    });

    it.only('ottoman - should create new doc', async () => {
        const Airplane = getOttomanModel();
        const options = { consistency: SearchConsistency.LOCAL };

        const stringAirplane = new Airplane(hawk);
        const createdString = await Airplane.create(stringAirplane);
        assertAirline(createdString, stringAirplane);
        assert.ok(typeof createdString.extension === 'string');

        const numberAirplane = new Airplane(eagle);
        const createdNumber = await Airplane.create(numberAirplane);
        assertAirline(createdNumber, numberAirplane);
        assert.ok(typeof createdNumber.extension === 'number');

        const booleanAirplane = new Airplane(falcon);
        const createdBoolean = await Airplane.create(booleanAirplane);
        assertAirline(createdBoolean, booleanAirplane);
        assert.ok(typeof createdBoolean.extension === 'boolean');

        const dateAirplane = new Airplane(sparrow);
        const createdDate = await Airplane.create(dateAirplane);
        assertAirline(createdDate, dateAirplane);
        // assert.ok(typeof createdString.scheduledAt === 'object');
        // assert.ok(typeof createdString.extension === 'object');
        // assert.ok(createdString.extension instanceof Date);

        const arrayAirplane = new Airplane(vulture);
        const createdArray = await Airplane.create(arrayAirplane);
        assertAirline(createdArray, arrayAirplane);
        assert.ok(typeof createdArray.extension === 'object');
        assert.deepStrictEqual(createdArray.extension, vulture.extension);

        const arrayObjdAirplane = new Airplane(bird);
        const createdArrayObj = await Airplane.create(arrayObjdAirplane);
        assertAirline(createdArrayObj, arrayObjdAirplane);
        assert.ok(typeof createdArrayObj.extension === 'object');
        assert.deepStrictEqual(createdArrayObj.extension, bird.extension);

        const objBird = { ...bird, extension: { external: 'field' } };
        const objectAirplane = new Airplane(objBird);
        const createdObject = await Airplane.create(objectAirplane);
        assertAirline(createdObject, objectAirplane);
        assert.ok(typeof createdObject.extension === 'object');
        assert.deepStrictEqual(createdObject.extension, objBird.extension);

        const find = await Airplane.find({}, options);
        console.log(find);
        assert.strictEqual(find.rows.length, 7);

        await removeDocuments();
    });
})