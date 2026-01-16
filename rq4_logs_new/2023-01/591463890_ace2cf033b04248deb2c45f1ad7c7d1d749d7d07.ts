export default function formatDate(date: string) {
    const stringToDate = new Date(date);
    const [day, _, year] = stringToDate.toLocaleDateString().split('.');

    const monthToString = stringToDate.toLocaleString("en-us", { month: "long"});

    const formatDay = (day: string) => {
        const dayToNumber = +day;

        if( isNaN(dayToNumber) ) return "1st";

        if( dayToNumber > 3 && dayToNumber < 21) return `${day}th`;

        switch ( dayToNumber % 10 ) {
            case 1:
                return `${day}st`;
            case 2:
                return `${day}nd`
            case 3:
                return `${day}rd`
            default:
                return `${day}th`
        }
    }

    const formattedDay = formatDay(day);

    return `${monthToString} ${formattedDay}, ${year}`;
}