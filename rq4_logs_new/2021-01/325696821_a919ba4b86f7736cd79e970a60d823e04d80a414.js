import Card from "@material-ui/core/Card";
import CardContent from "@material-ui/core/CardContent";
import Typography from "@material-ui/core/Typography";
import React from "react";

const Forecast = (back, forecast, city, country) => {

    function minmaxTemp(min, max, feel) {
        return (
            <div style={{ display: 'flex', flexDirection: 'row', justifyContent: 'center'}}>
                <div>
                    <Typography variant="overline">Feels</Typography>
                    <Typography variant="h4">{feel}&deg;</Typography>
                </div>
                <div style={{width: '2vw'}}></div>
                <div>
                    <Typography variant="overline">Low</Typography>
                    <Typography variant="h4">{min}&deg;</Typography>
                </div>
                <div style={{width: '2vw'}}></div>
                <div>
                    <Typography variant="overline">High</Typography>
                    <Typography variant="h4">{max}&deg;</Typography>
                </div>
            </div>
        );
    }

    return (
        <div className="container" style={{marginTop: '5vh'}}>
            <p>{forecast}</p>
            <Card>
                <CardContent>
                    <Typography variant="h2">{city}</Typography>
                    <Typography variant="subtitle2">{country}</Typography>
                    <Typography variant="h3" style={{marginTop: '3vh'}}><i className="wi wi-day-sunny display-1" /></Typography>
                    <Typography variant="overline" style={{marginTop: '3vh'}}>light rain</Typography>
                    {/*<Typography variant="h3" style={{marginTop: '3vh'}}>{forecast.daily[0].temp.day}&deg;<span style={{color: 'grey'}}>{forecast.daily}&deg;</span></Typography>*/}
                    {minmaxTemp(25, 30)}

                </CardContent>
            </Card>
        </div>
    );
}

export {Forecast};