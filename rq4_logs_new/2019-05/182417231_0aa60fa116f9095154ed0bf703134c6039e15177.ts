import { FlatMap } from '../../shared/utils';

export enum Load {
    Base,
    ThreeDays,
    FiveDays,
    FiveDaysIntensive,
    EveryDay,
    EveryDayIntensive,
    EveryDayAndWork,
}

export const load = new FlatMap([
        { key: Load.Base, name: 'PAGES.PROFILE.FORM.LOAD.BASE' },
        { key: Load.ThreeDays, name: 'PAGES.PROFILE.FORM.LOAD.THREE_DAYS' },
        { key: Load.FiveDays, name: 'PAGES.PROFILE.FORM.LOAD.FIVE_DAYS' },
        { key: Load.FiveDaysIntensive, name: 'PAGES.PROFILE.FORM.LOAD.FIVE_DAYS_INTENSIVE' },
        { key: Load.EveryDay, name: 'PAGES.PROFILE.FORM.LOAD.EVERY_DAY' },
        { key: Load.EveryDayIntensive, name: 'PAGES.PROFILE.FORM.LOAD.EVERY_DAY_INTENSIVE' },
        { key: Load.EveryDayAndWork, name: 'PAGES.PROFILE.FORM.LOAD.EVERY_DAY_AND_WORK' },
    ], 'key');