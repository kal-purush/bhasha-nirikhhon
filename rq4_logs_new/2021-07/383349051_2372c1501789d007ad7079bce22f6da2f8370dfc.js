import React from 'react'
import{
    Navbar,
    NavbarBrand,
    Nav,
    NavItem,
    NavLink
} from 'reactstrap';
import Auth from './SignUp'

const Landing = () => {
    return(
        <div>
            <div id="landing-header">
            <Navbar className='header'>
                <NavbarBrand href ='/'>Quick Scripts</NavbarBrand>
                <NavLink href=''>User Dashboard</NavLink>
                <Nav className='ml-auto' navbar>

                    <form class="container-fluid justify-content-start">
                    <button class="btn btn-outline-success me-2" type="button">Sign Up</button>
                    <button class="btn btn-outline-success me-2" type="button">Log In</button>
                    </form>

                </Nav>
            </Navbar>
            </div>

            <div id="landing-body">

                <div class="card bg-dark text-white">
                <img  src="https://images.unsplash.com/photo-1576091160399-112ba8d25d1d?ixid=MnwxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8&ixlib=rb-1.2.1&auto=format&fit=crop&w=1050&q=80" class="card-img" alt="pharmicist using technology for prescription pickups" />

                <div class="card-img-overlay">
                <h5 class="card-title">Technology enhancing healthcare</h5>
                <p class="card-text">Access affordable healthcare with easy online transactions</p>
                <p class="card-text">Try it today</p>

                <div class="input-group mb-3">
                <input type="text" class="form-control" placeholder="Type a medication" aria-label="Example text with button addon" aria-describedby="button-addon1" />
                <button class="btn btn-outline-secondary" type="button" id="button-addon1">Search</button>
                </div>
                </div>


                </div>

            </div>

            <Auth />
        </div>
    );
};

export default Landing;