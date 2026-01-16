import styles from "./Analog.module.css";
export default function AnalogClock({hours,minutes,seconds}){
    const minuteIndicator = Array.from({length:60},(i,idx)=>idx);
    const time = Array.from({length:12},(i,idx)=>idx+1);
    const weekDays = ["Sun","Mon","Tue","Wed","Thu","Fri","Sat"];
    function getTop(idx){
        let total = 0;
        for(let i = 0;i < idx; i++){
            if(i%5 === 0){
                total += 4;
            }
            else{
                total += 2;
            }
        }
        return total+108;

    }
    function getHourAngle(){
        return (((hours*3600)+(minutes*60)+seconds)/3600)*30;
    }
    function getMinuteAngle(){
        return (((minutes*60)+seconds)/60)*6;
    }
    return(
        <div>
            <div className={styles.clock}>
                <div className={styles.hourHand} style={{
                    rotate:`${getHourAngle()}deg`
                }}></div>
                <div className={styles.minuteHand} style={{
                    rotate:`${getMinuteAngle()}deg`
                }}></div>
                <div className={styles.secondHand} style={{
                    rotate:`${seconds*6}deg`
                }}></div>
                {minuteIndicator.map((i,idx)=>{
                    return(
                        <div className={styles.indicator} style={{
                            rotate:`${idx*6}deg`,
                            zIndex:`${3+idx}`,
                            top:`-${getTop(idx)}%`,
                            height:idx%5 === 0 ? "4%" : "2%",
                            transformOrigin:idx%5 === 0 ? `50% ${5000/4}%` : `50% ${5000/2}%`

                        }}></div>
                    )
                })}
                {time.map((i,idx)=>{
                    return(
                        <div className={styles.time} style={{
                            rotate:`${idx*30}deg`,
                            zIndex:`${63+idx}`,
                            top:`-${249 + idx*10}%`,
                            transformOrigin:`50% ${4700/10}%`
                        }}>
                            <div style={{
                                height:"100%",
                                width:"100%",
                                rotate:`-${idx*30}deg`,
                                transformOrigin:"50% 50%",
                                display:"grid",
                                placeItems:"center"
                            }}>{idx === 0 ? 12 : idx}</div>
                        </div>
                    )
                })}
                <div className={styles.centerOfClock}></div>
                <div className={styles.dateAndDay}>
                    <div className={styles.day}>{weekDays[new Date().getDay()]}</div>
                    <hr/>
                    <div className={styles.date}>{new Date().getDate()}</div>
                </div>
            </div>
        </div>
    )
}