import React from 'react';

import Search from './subcomponent/search'

class Navbar extends React.Component {

    gatoRef = '#'
    render(){ 
        return (
                <div className='navbar navbar-expand-sm navbar-dark bg-dark'>
                    <a className='navbar-brand' href={this.gatoRef} >unlogo</a> 

                    <ul className='navbar-nav'>               
                        <li className='nav-item'><a className='nav-link'href={this.gatoRef}>inicio</a></li>
                        <li className='nav-item'><a className='nav-link'href={this.gatoRef}>medio</a></li> 
                        <li className='nav-item'><a className='nav-link'href={this.gatoRef}>finalgf</a></li>
                    </ul>
                    <div>
                        <Search searchApi={this.props.searchApi}/>
                    </div>
                </div >
        )
    }
}

export default Navbar