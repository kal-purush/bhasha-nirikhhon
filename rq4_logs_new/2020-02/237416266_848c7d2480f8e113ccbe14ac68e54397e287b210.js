import React from "react";
import CountUp from "react-countup";

export default function IncreasingNumber(props) {
    return (
            <CountUp
                start={0}
                end={props.max}
                duration={2.75}
                decimals={props.decimals ? props.decimals : 0}/>
    )
};