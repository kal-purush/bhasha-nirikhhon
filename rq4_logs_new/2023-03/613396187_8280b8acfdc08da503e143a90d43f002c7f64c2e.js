import { useState } from 'react';
import { Link } from 'react-router-dom';
import { HiUserCircle} from 'react-icons/hi';
import { useNavigate } from 'react-router-dom';

function Navbar({user}) {
  const [isCollapsed, setIsCollapsed] = useState(true);
  const navigate = useNavigate()

  function toggleNavbar() {
    setIsCollapsed(!isCollapsed);
  }

  const logout = ()=> {
    localStorage.removeItem('accessToken')
    localStorage.removeItem('refreshToken')
    navigate('/')
  }

  const handleProfileClick = () => {
    if (!localStorage.getItem('accessToken')) {
      alert('Please log in to view your profile');
      return;
    }
  }

  return (
    <nav className="navbar navbar-expand-lg navbar-dark cardtop centered fixed-top">
      <div className="container-fluid">
        <Link className="navbar-brand" to="/home">
          <img src="https://www.cytekia.com/assets/imgs/logo.png" alt="Logo" style={{ width: "200px", height: "auto" }} />
        </Link>
        <button className="navbar-toggler" type="button" onClick={toggleNavbar} aria-label="Toggle navigation">
          <span className="navbar-toggler-icon"></span>
        </button>
        <div className={`collapse navbar-collapse ${isCollapsed ? "" : "show"}`} id="navbarNav">
        <ul className="navbar-nav ms-auto">
        <li className="nav-item me-3">
              <Link onClick={handleProfileClick} className="nav-link" to="/profile">
                <i className="bi bi-person-fill"><HiUserCircle size={25} color="#fff" style={{marginRight: "5px"}}/>
                     {localStorage.getItem('accessToken') ? ( user.name) : "Profile"}
                </i> 
              </Link>
            </li>
            {localStorage.getItem('accessToken') ? (
          <li className="nav-item">
            <button onClick={logout} className="btn login btn-outline-light">
              Logout
            </button>
          </li>
        ) : null}
          </ul>
        </div>
      </div>
    </nav>
  );

}
export default Navbar;