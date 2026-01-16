import React from "react";
import './Header.css';
import logo from '../../../images/yuyuyu.svg';
import menu_svg from '../../../images/menu.svg';
import $ from 'jquery'

export default class Header extends React.Component {
    componentDidMount(){    
        $(function () {

            // show hide subnav depending on scroll direction
            var position = $(window).scrollTop();
        
            $(window).scroll(function () {
                var scroll = $(window).scrollTop();
        
                if (scroll > position) {
        
                    //only piece that matters
                    $('nav')
                        .stop(true, false)
                        .removeClass('animateIn')
                        .addClass('animateOut');
                    // scrolling downwards, only here for dev purposes
                    // console.log('moving DOWN the page');
        
                } else {
                    //only piece that matters
                    $('nav')
                        .stop(true, false)
                        .removeClass('animateOut')
                        .addClass('animateIn');        
                    // scrolling upwards 
                    // console.log('moving UP the page');
                }
        
                position = scroll;
            });
        
        });
    }

    render() {
        return (
            <nav className="navbar">
                <div className="logo">
                     <a href="/">
                         <img src={logo} alt="Logo of Yuyuyu Design"/>
                    </a>
                </div>
                <div className="menu">
                    <div className="disabled">Work</div>
                    <div className="disabled">About</div>
                    <div className="disabled">Contact</div>
                </div> 
                <div className="menu-svg">
                    <img src={menu_svg} alt="menu" />
                </div>
            </nav>
        );
    }
}