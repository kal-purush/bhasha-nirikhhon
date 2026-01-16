import React from "react";
import no_img from "../../images/person-icon.png"

function User({user}) {
    return (
        <div className="user">
            <div className="user__img">
                <img src={user.img === "" ? no_img : user.img} alt=""/>
            </div>
            <div className="user__name">
                {user.firstName} {user.lastName}
            </div>
            <div className="user__location">
                {user.location.city}, {user.location.country}
            </div>
            <div className="user__addFriend">
                <button className="btn btn-primary">{user.isFriend ? "Add" : "Remove"}</button>
            </div>
        </div>
    )
}

const FriendsPage = (props) => {
    return (
        <div className="friends">
            {props.users.map(user => <User key={user.id} user={user}/>)}
        </div>
    )
}

export default FriendsPage