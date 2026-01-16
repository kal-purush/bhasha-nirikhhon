import React from 'react';
import SOCIAL_PROFILES from '../data/soscialProfiles';



const SocialProfile = props =>{
    const {link, image} = props.SocialProfile;
    return (
      <span>
      <a href={link} target="_blank" rel="noopener noreferrer">
        <img src={image} alt="Social Profile" style={{width:30, height:30, margin:8}} />
      </a>
    </span>
    )
}

  export default function(){
    return(
      <div style={{display:'flex',flexDirection:'row',justifyContent:'center', alignItems:'center',height:'50px',width:'100%', position:'fixed',bottom:'0',}}>

        {
          SOCIAL_PROFILES.map(SOCIAL_PROFILE =>{
            return <SocialProfile key={SOCIAL_PROFILE.id} SocialProfile={SOCIAL_PROFILE}/>
          })
        }
        <h3 style={{fontFamily:'Segoe UI'}}>Connect with me!</h3>
      </div>
    )
  }