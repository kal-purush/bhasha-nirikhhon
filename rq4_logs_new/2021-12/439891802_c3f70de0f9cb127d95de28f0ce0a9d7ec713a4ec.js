import moment from 'moment';

export default function WeatherHourDetailsTile({weather}) {

    return (
        <div className='weather-day-details-tile'>
            <p>{moment.unix(weather.dt).format("h:mm")} {weather.main.temp} º</p> 
        </div>
    )
}