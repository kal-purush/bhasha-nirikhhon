import React from 'react'
import './Banner.css'
import { Button } from '@material-ui/core';
import { useHistory } from "react-router-dom";




function Banner() {
    const history = useHistory();

    return (
        <div className='banner'>
            <div className='banner_search'>
                <Button 
                className='banner_searchButton' varient='outlined'>Events Dates</Button>
            </div>
            <div className='banner_info'>
                <h1>Get out and meet new people</h1>
                <h5>
                    Now payments for tickets are online ,Hurry up !
                </h5>
                <Button onClick={() => history.push ('/search')} variant='outlined'>Explore Nearby</Button>
            </div>
        </div>
    )
}
 
export default Banner