/*
 * File: TeamPage.js
 * Project: frontend
 * File Created: Friday, 28th May 2021 1:15:26 am
 * Author: Vithushan Sylvester (vsylvester@mitrai.com)
 * -----
 * Last Modified: Friday, 28th May 2021 2:23:38 am
 * Modified By: Vithushan Sylvester (vsylvester@mitrai.com)
 * -----
 * Copyright 2021 vithushan sylvester
 */
import { React, useEffect, useState } from 'react';
import TeamService from '../services/IplTeamService'

export const TeamPage = () => {
    const [team, setState] = useState();

    useEffect(() => {
        const getData = async () =>  {
            let data = await TeamService.getTeamByTeamName("Delhi%20Capitals", 5);
            console.log(data)
            setState(data);
        }
        getData();
    });

    return (<div className="TeamPage">
        <h1>team.teamName</h1>
    </div>)
}