import React from 'react'
import {Image } from 'react-bootstrap'
import './DetailMaisonAdmin.css';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome'
import { faBed, faGlobe, faHouse, faLocationDot, faSink, faTag } from '@fortawesome/free-solid-svg-icons'
import SideBars from '../../Components/SideBars/SideBars'
import NavbarAdmin from '../../Components/Navbars/NavbarAdmin/NavbarAdmin'
import Tableaux from '../Tableaux/Tableaux'
import profilemaison from '../../fichiers/bann accueil.jpeg'

 
export default function DetailMaisonAdmin() {
  return (
    <div>
     <div className="">
      <div
        className='maincontent-dashbord-static'
      >
        <div className="contentsidebar">
          <SideBars
            
          />
        </div>
        <div className="secondecontent">
          <div className="">
            <NavbarAdmin/>
          </div>
          <Tableaux />
            <div id='content-main-detail-maison-content-admin' className='mt-2 container '>
                <div className='d-flex content-main-detailmaison container '>
                    <div className='one-img-left'h-100  >
                        <div className='img-main-detail-maison'>
                            <Image  src={profilemaison} className='content-img-detail-maison1' />
                        </div>
                        
                    </div>
                    <div className='second-img-right h-100 '>
                    <div className='second-img-right1'>
                        <h1>Maison</h1>
                        <p>Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the 
                        industry's standard dummy text ever since the 1500s,</p>
                        <div className='d-flex justify-content-evenly'>
                            <div>
                                <div><span><FontAwesomeIcon icon={faGlobe} id='icon-details-right' /></span><span>150m2</span></div>
                                <div><span><FontAwesomeIcon icon={faTag} id='icon-details-right' /></span><span>50.000.000FCFA</span></div>
                                <div><span><FontAwesomeIcon icon={faLocationDot} id='icon-details-right' /></span><span>Diass 123 rue Birago Diop</span></div>
                            </div>
                            <div>
                                <div><span><FontAwesomeIcon icon={faHouse} id='icon-details-right' /></span><span>R+4</span></div>
                                <div><span><FontAwesomeIcon icon={faBed} id='icon-details-right' /></span><span>10 CHAMBRES</span></div>
                                <div><span><FontAwesomeIcon icon={faSink} id='icon-details-right' /></span><span>10 Salles de bains</span></div>
                            </div>
                        </div>
                      </div>
                    </div>
                </div>
            </div>
        </div>
      </div>
    </div>
 </div>
  )
}