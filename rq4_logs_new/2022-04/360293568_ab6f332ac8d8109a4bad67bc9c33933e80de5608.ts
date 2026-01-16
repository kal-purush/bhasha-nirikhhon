import ISafeplace from './interfaces/ISafeplace';

interface TimetableDay {
    name: string;
    timetable: string[];
    isChecked: boolean;
}

const formatTime = (strTime: string) => {
    if (strTime === undefined)
        return "";
    if (strTime.includes('h')) {
        if (strTime.split('h')[0].length === 1)
            strTime = `0${strTime}`;
        if (strTime.split('h')[1].length === 0)
            return strTime.replace('h', ':00');
        else return strTime.replace('h', ':');
    }
};

const isAfternoon = (strTime: string) => {
    if (strTime === undefined) return false;
    return (Number(strTime.split(':')[0]) > 12);
};

const splitDayTimetable = (dayTimetable: string | null): string[] => {
    let daySchedule = new Array(4).fill("");

    if (dayTimetable === null)
        return daySchedule;

    dayTimetable.split(',').forEach((element, index) => {
        const hoursSplit = element.split(' à ');
        daySchedule[index * 2] = formatTime(hoursSplit[0]);
        daySchedule[index * 2 + 1] = formatTime(hoursSplit[1]);
    });

    if (daySchedule[2] === "" &&
        daySchedule[3] === "" &&
        isAfternoon(daySchedule[0]) &&
        isAfternoon(daySchedule[1])
    ) {
        daySchedule[2] = daySchedule[0];
        daySchedule[3] = daySchedule[1];
        daySchedule[0] = "";
        daySchedule[1] = "";
    }

    return daySchedule.map(element =>
        element === undefined ? "" : element);
};

const getTimetableFromSafeplace = (safeplace: ISafeplace): TimetableDay[] => {
    const timetable = safeplace.dayTimetable;
    const days = [
        "Lundi",
        "Mardi",
        "Mercredi",
        "Jeudi",
        "Vendredi",
        "Samedi",
        "Dimanche"
    ];

    return timetable
        .map(element => splitDayTimetable(element))
        .map((day, index) => ({
            name: days[index],
            timetable: day,
            isChecked: day.filter(element => element !== "").length > 0
        }));
};

const joinTimetable = (dayTimetable: TimetableDay[]): (string | null)[] => {
    return dayTimetable
        .map(element => (!element.isChecked) ? { ...element, timetable: new Array(4).fill("") } : element)
        .map(element => {
            const validTimes = element.timetable.filter(elT => elT !== "");

            if (validTimes.length === 0)
                return null;

            const firstPart = validTimes.slice(0, 2).map(t => t.replace(':', 'h')).join(' à ');
            const secondPart = validTimes.slice(2, 4).map(t => t.replace(':', 'h')).join(' à ');

            return (validTimes.length > 2)
                ? `${firstPart},${secondPart}`
                : firstPart;
        });
};

const updateSafeplaceWithTimetable = (
    safeplace: ISafeplace,
    timetable: TimetableDay[]
): ISafeplace => {
    return {
        ...safeplace,
        dayTimetable: joinTimetable(timetable)
    };
};

export type { TimetableDay };
export {
    getTimetableFromSafeplace,
    updateSafeplaceWithTimetable
};