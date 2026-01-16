const makeTimeHumanReadable = (input: number): string => {
    const ms = input % 1;
    const s = Math.floor(input) % 60;
    const m = Math.floor(input / 60) % 60;
    const h = Math.floor(input / 3600);

    const msString = ms === 0 ? "" : ms.toFixed(3).slice(1);

    const sString = s < 10 ? `0${s}` : `${s}`;

    const mString = m < 10 ? `0${m}` : `${m}`;
    const hString = h === 0 ? "" : `${h}:`;

    return `${hString}${mString}:${sString}${msString}`;
};
export default makeTimeHumanReadable;