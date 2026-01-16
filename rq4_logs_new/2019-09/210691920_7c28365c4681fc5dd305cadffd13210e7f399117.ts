import {Carrier, Coordinate, Cruiser, Destroyer, Orientation, WarshipClass, WarshipPartStatus} from "..";

describe('Warship - Base Behavior', () => {
    it('should be able to be create', function () {
        let ussCalister = new Carrier();

        expect(ussCalister.class()).toBe(WarshipClass.CARRIER);
        expect(ussCalister.size()).toBe(4);

        expect(ussCalister.partStatus()).toStrictEqual(Array(4).fill(WarshipPartStatus.NOMINAL));

        ussCalister.setPosition(new Coordinate(4, 5), Orientation.NORTH);

        expect(ussCalister.head()).toBeInstanceOf(Coordinate);
        expect(ussCalister.head()).toStrictEqual(new Coordinate(4, 5));
        expect(ussCalister.orientation()).toBe(Orientation.NORTH);
    });

    it('should be able to be hit', function () {
        let ussEnterprise = new Destroyer();
        ussEnterprise.setPosition(new Coordinate(10, 5), Orientation.SOUTH);

        expect(ussEnterprise.hasBeenHit(new Coordinate(0, 0))).toBe(false);
        expect(ussEnterprise.partStatus()).toStrictEqual(Array(ussEnterprise.size()).fill(WarshipPartStatus.NOMINAL));

        expect(ussEnterprise.hasBeenHit(new Coordinate(10, 3))).toBe(true);
        expect(ussEnterprise.partStatus()).toStrictEqual([
            WarshipPartStatus.NOMINAL,
            WarshipPartStatus.NOMINAL,
            WarshipPartStatus.HIT,
        ]);
        expect(ussEnterprise.isAlive()).toBe(true);
    });

    it('should be able to be destroyer', function () {
        let ussCrusader = new Cruiser();
        ussCrusader.setPosition(new Coordinate(10, 5), Orientation.WEST);

        expect(ussCrusader.hasBeenHit(new Coordinate(0, 0))).toBe(false);
        expect(ussCrusader.partStatus()).toStrictEqual([WarshipPartStatus.NOMINAL, WarshipPartStatus.NOMINAL]);

        expect(ussCrusader.hasBeenHit(new Coordinate(10, 5))).toBe(true);
        expect(ussCrusader.partStatus()).toStrictEqual([
            WarshipPartStatus.HIT,
            WarshipPartStatus.NOMINAL,
        ]);
        expect(ussCrusader.isAlive()).toBe(true);

        expect(ussCrusader.hasBeenHit(new Coordinate(9, 5))).toBe(true);
        expect(ussCrusader.partStatus()).toStrictEqual([
            WarshipPartStatus.HIT,
            WarshipPartStatus.HIT,
        ]);
        expect(ussCrusader.isAlive()).toBe(false);
    });
});