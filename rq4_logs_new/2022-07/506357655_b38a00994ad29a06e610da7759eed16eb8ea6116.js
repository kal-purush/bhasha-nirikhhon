import { useState, useContext, useEffect } from "react";

import { AuthContext } from "../../contexts/AuthContext";
import { isAuth } from "../../HOC/isAuth";
import * as profileService from '../../services/profileService';


function Comments({comment}) {

    let [profile, setProfile] = useState([]); 

    let { user } = useContext(AuthContext); 


    useEffect(() => {
        profileService.getProfile(user.accessToken, comment._ownerId)
        .then(result => {
            setProfile(result[0])
        })

    }, [user.accessToken, comment._ownerId]);

    if(profile == undefined) {
        return <p>Loading..</p>
    }

    console.log(profile);

    return (
        <div className="comment-container">
            <p>{profile.username} says: {comment.comment}</p>
           
        </div>
    )
       
    

    

    



return (
    <div className='all-comments'>
       
    </div>

    )
}
export default isAuth(Comments);