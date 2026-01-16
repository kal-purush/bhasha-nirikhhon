import React,{Component} from "react";
import Img from "../../assets/banner920.jpg";

import './style.css';

class Banner extends Component{
    render(){
        return (
            <img src={Img}/>
        );
    }
}
export default Banner;