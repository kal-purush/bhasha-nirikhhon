import React, { Component } from "react";
import classes from './Calls.module.css'
import Web from './web.png'
import Vector from './Vector.png'
import micro from './micro.png'
import Icon from './Icon.png'

class Calls extends Component{
    render(){
        return(
            <div className={classes.Deks}>
                <div className={classes.WebCam}>
                    <img src={Web}/>
                    <div className={classes.Block}>
                        <div className={classes.UpBlock}>
                            <img src={Vector}/>
                            <h1>Текущий звонок</h1>
                        </div>
                        <div className={classes.Button_List}>
                                
                                <div className={classes.CardPeop}>
                                <div className={classes.Group}>
                                <img src={Icon} style={{
                                    maxHeight: '75px'
                                }}/>
                                <div className={classes.InfoPeop}>
                                    <h1>Сергей</h1>
                                    <h2>Дада</h2>
                                    
                                </div>
                                
                                </div>
                                <img src={micro}/>
                                </div>
                                
                        </div>
                    </div>
                </div>
            </div>
        )
    }
}

export default Calls