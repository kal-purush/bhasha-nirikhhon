import format from "date-fns/format";
import React from "react";

export default function (eventDate) {
    return <>{format(eventDate, 'dd')}/{format(eventDate, 'MM')}</>
}