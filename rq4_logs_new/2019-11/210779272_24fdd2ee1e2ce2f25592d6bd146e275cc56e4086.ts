import sequelize from 'sequelize';

import {
    extractCount,
    setIncludeableArchived,
    setFindOptsArchived,
} from '../../../../src/models/helpers/options';

describe('extractCount', () => {
    it('extractCount_EmptyObject_EmptyObject', () => {
        expect(extractCount({ where: {} })).toEqual({
            where: {},
            paranoid: true,
            include: undefined,
        });
    });

    it('extractCount_Paranoid_WithParanoid', () => {
        expect(extractCount({ where: {}, paranoid: false }, [])).toEqual({
            where: {},
            paranoid: false,
            include: [],
        });
    });
});

describe('setIncludeableArchived', () => {
    it('setIncludeableArchived_include_paranoidApplied', () => {
        const include = {};
        expect(setIncludeableArchived(include)).toEqual({ paranoid: false });
    });

    it('setIncludeableArchived_deepInclude_paranoidApplied', () => {
        const include = { include: [{}] };
        expect(setIncludeableArchived(include)).toEqual({
            paranoid: false,
            include: [{ paranoid: false }],
        });
    });
});

describe('setFindOptsArchived', () => {
    it('setFindOptsArchived_include_paranoidApplied', () => {
        const include = {};
        expect(setFindOptsArchived(include)).toEqual({
            paranoid: false,
            where: { deletedAt: { [sequelize.Op.ne]: null } },
        });
    });

    it('setFindOptsArchived_deepInclude_paranoidApplied', () => {
        const include = { include: [{}] };
        expect(setFindOptsArchived(include)).toEqual({
            paranoid: false,
            where: { deletedAt: { [sequelize.Op.ne]: null } },
            include: [{ paranoid: false }],
        });
    });

    it('setFindOptsArchived_deepIncludeAndWhereAlreadyDefined_paranoidApplied', () => {
        const include = { include: [{}], where: { test: 1 } };
        expect(setFindOptsArchived(include)).toEqual({
            paranoid: false,
            where: { deletedAt: { [sequelize.Op.ne]: null }, test: 1 },
            include: [{ paranoid: false }],
        });
    });
});