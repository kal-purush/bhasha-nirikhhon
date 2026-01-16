import React from 'react';
import '../../css/pages/Team.scss';
import TeamGroup from './team/TeamGroup';

const Team = () => {
    const Teams = {teams: [
        {
            id: 0,
            teamName: 'Project Leaders',
            members: [
                {
                    name: "seasandsurf",
                    position: "",
                    avatar: "https://cdn.discordapp.com/avatars/430290357410988032/ca7a6b799bb6331ba09e20bd8a8193e5.webp"
                },
                {
                    name: "HMAS_Lightning",
                    position: "",
                    avatar: "https://cdn.discordapp.com/avatars/262834639993831424/b061aacabc7cfbc687def580f1867145.png"
                }
            ]
        },
        {
            id: 0,
            teamName: 'Department Leaders',
            members: [
                {
                    name: "seasandsurf",
                    position: "",
                    avatar: "../../img/seas@2x.png"
                },
                {
                    name: "seasandsurf",
                    position: "",
                    avatar: "../../img/seas@2x.png"
                },
                {
                    name: "seasandsurf",
                    position: "",
                    avatar: "../../img/seas@2x.png"
                },
                {
                    name: "seasandsurf",
                    position: "",
                    avatar: "../../img/seas@2x.png"
                },
                {
                    name: "seasandsurf",
                    position: "",
                    avatar: "../../img/seas@2x.png"
                },
                {
                    name: "seasandsurf",
                    position: "",
                    avatar: "../../img/seas@2x.png"
                }
            ]
        }
    ]}; 
    const TeamRender = [];
    Teams.teams.forEach(teamGroup => {
        TeamRender.push(<TeamGroup team={teamGroup}/>);
    })
        return(
            <team>
                <h1>The Team</h1>
                {TeamRender}
            </team>
        );
    
}

export default Team;