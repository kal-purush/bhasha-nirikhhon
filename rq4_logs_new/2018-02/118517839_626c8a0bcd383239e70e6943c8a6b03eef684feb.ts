import {RegisterTeamMember} from './RegisterTeamMember'

export class RegisterTeam {
    constructor(public name: string,
        public description: string,
        public teamLeaderId: string,
        public teamManagerId: string,
    public teamMembers: RegisterTeamMember[]) { }
}