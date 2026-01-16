import {VolunteerAccount} from "../models/VolunteerAccount";


enum BadgeType {
    Organizer,
    Profile,
    Connector,
    Supporter,
    Leader,
    Participation
}


function getBadgeName() {

}

function getBadgeDescription() {

}

async function updateSupporterBadge(email : string) {
    const res = await fetch(`/api/volunteeraccounts/${email}`, {
        method: "GET",
      })
    const user = (await res.json()).data as VolunteerAccount;
    user.commentsPosted++;
    if (user.commentsPosted >= 15)    
        user.badges.Supporter = 4
    else if (user.commentsPosted >= 10)
        user.badges.Supporter = 3
    else if (user.commentsPosted >= 5)
        user.badges.Supporter = 2
    else if (user.commentsPosted >= 1)
        user.badges.Supporter = 2
    const update = await fetch(`/api/volunteeraccounts/${email}`, {
        method: "PATCH",
        body: JSON.stringify(user),
    });

}

export {updateSupporterBadge}