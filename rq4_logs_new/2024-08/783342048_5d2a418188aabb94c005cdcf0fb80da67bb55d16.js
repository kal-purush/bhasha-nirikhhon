import { useState, useEffect } from 'react';
import './svateOmse.css';

var times = {};

async function parseTimeable() {
    var timetable = await fetch(process.env.PUBLIC_URL + "/svateOmse.csv").then(res => res.text())
    timetable.split("\n").forEach(line => {
        if (line.startsWith("//")) return;
        times[line.split(",")[0].trim()] = Array.from(line.split(",").slice(1)).map(el => el.trim())
    });
    return times;
}



export default function SvateOmse() {
    let [times, setTimes] = useState(null);

    useEffect(() => {
        parseTimeable().then((newTimes) => {
            setTimes(newTimes);
        });
    }, []);

    return (
        <fieldset className='massTableContainer'>
            <legend>Sväté omše</legend>
            {times ? (
                <table>
                    <tbody>
                        {Object.entries(times).map(([day, times]) => (
                            <tr key={day}>
                                <th>{day}</th>
                                <td>
                                    {times.length === 0 ? "—" : times.join(" ")}
                                </td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            ) : (
                <h1>Načítavajú sa dáta...</h1>
            )}
        </fieldset>
    );
}