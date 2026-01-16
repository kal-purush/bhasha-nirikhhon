import React from 'react'
import Indianflag from "../../images/flags/in.svg";

const Teamcomponent = ({imageUrl,Name,Designation,Link}) => {
    return (
		

                    <div className="freelancer">
                    <div className="freelancer-overview">
                        <div className="freelancer-overview-inner">
                            
                            {/* <!-- Avatar --> */}
                            <div className="freelancer-avatar">
                                <a href={Link}><img src={imageUrl} alt=""/></a>
                            </div>

                            {/* <!-- Name --> */}
                            <div className="freelancer-name">
                                <h4><a href={Link}>{Name} <img className="flag" src={Indianflag} alt="" title="India" data-tippy-placement="top"/></a></h4>
                                <span>{Designation}</span>
                            </div>
                        </div>
                        <div className="our-team-button">
                            <a href={Link} className="button button-sliding-icon ripple-effect text-center">Know More <i className="icon-material-outline-arrow-right-alt"></i>
                            </a>
                        </div>
                    </div>
                    </div>
                    
              
    )
}

export default Teamcomponent