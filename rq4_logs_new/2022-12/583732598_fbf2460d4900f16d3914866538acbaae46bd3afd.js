import {Collapse} from 'bootstrap';


function Navbar () {
  return (
      <nav class="navbar navbar-expand-md bg-light navbar-light shadow-sm p-3 mb-5 bg-body rounded">
        <button class="navbar-toggler" type="button" data-bs-toggle="collapse" data-bs-target="#coll" >
          <span class="navbar-toggler-icon"></span>
          </button> 

          <div class="collapse navbar-collapse" id="coll">
            <ul class="navbar-nav">
            <li class="nav-item">
              <a href="#" class="nav-link">About</a>
            </li>

            <li class="nav-item">
              <a href="#" class="nav-link">Service</a>
            </li>

            <li class="nav-item">
              <a href="#" class="nav-link">Gallery</a>
            </li>

            <li class="nav-item">
              <a href="#" class="nav-link">Contact Us</a>
            </li>
            </ul>
          </div>
          </nav>
  )
}


export default Navbar;