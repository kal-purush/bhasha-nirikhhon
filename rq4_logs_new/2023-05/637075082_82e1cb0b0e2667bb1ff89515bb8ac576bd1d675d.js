
import React from "react";

const navbar=(props)=> {

          return(
            <div style={styles.nav}>
                <div style={styles.navIconContainer}>
                    <img style={styles.navIcon} src="https://img.freepik.com/free-icon/carts_318-676736.jpg?size=626&ext=jpg" alt="cart"/>
                    <span style={styles.cartCount}>{props.count}</span>
                </div>
            </div>
        );
    }

const styles={
    nav:{
        height: 70,
        background:'#1385E0',
        display:'flex',
        justifyContent:'flex-end',
        alignItems:'center'
        
    },
    navIcon:{
        height:30,
        marginRight:20
    },
    navIconContainer:{
        position:'relative'
    },
    cartCount:{
        position:'absolute',
        borderRadius:'50%',
        width:20,
        right:13,
        top:-9,
        background:'yellow',
        textAlign:'center'
    }
}

export default navbar;