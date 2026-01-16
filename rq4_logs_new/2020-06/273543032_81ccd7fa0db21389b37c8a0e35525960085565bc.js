import React from 'react'
import PropTypes from 'prop-types'

import Container from './styles'
import RestaurantIcon from '@material-ui/icons/Restaurant';
import RestaurantMenuIcon from '@material-ui/icons/RestaurantMenu';
import PhotoCameraIcon from '@material-ui/icons/PhotoCamera';


import MenuBookIcon from '@material-ui/icons/MenuBook';
import RoomServiceIcon from '@material-ui/icons/RoomService';
import { toast } from "react-toastify";
import { logout } from '../../services/auth'
import ReportIcon from '@material-ui/icons/Report';
import MonetizationOnIcon from '@material-ui/icons/MonetizationOn';
import MotorcycleIcon from '@material-ui/icons/Motorcycle';
import AssignmentTurnedInIcon from '@material-ui/icons/AssignmentTurnedIn';
import CancelIcon from '@material-ui/icons/Cancel';
import PauseCircleFilledIcon from '@material-ui/icons/PauseCircleFilled';
import ExitToAppIcon from '@material-ui/icons/ExitToApp';

function handleLogout () {
	logout();
	
	toast.error("Logout Realizado, realize login para continuar.");
	window.location.reload(false);
  }

function SideBar ({ page, changePage }) {
  return (
    <Container page={page}>
      <li 
        className={page === 'Orders' ? 'active' : ''}
        onClick={() => changePage('Orders')} 
      >
       <RoomServiceIcon style={{ fontSize: 40 }} /><br/> Todos os Pedidos
      </li>
      <li
        className={page === 'Pendentes' ? 'active' : ''}
        onClick={() => changePage('Pendentes')} 
      >
        <ReportIcon style={{ fontSize: 40 }} /><br/>Pendentes
      </li>
     
      <li
        className={page === 'Pagos' ? 'active' : ''}
        onClick={() => changePage('Pagos')}
      >
       <MonetizationOnIcon style={{ fontSize: 40 }} /><br/> Pagos
      </li>
      <li
        className={page === 'Enviados' ? 'active' : ''}
        onClick={() => changePage('Enviados')}
      >
       <MotorcycleIcon style={{ fontSize: 40 }} /><br/> Enviados
      </li>
	  <li
        className={page === 'Finalizados' ? 'active' : ''}
        onClick={() => changePage('Finalizados')}
      >
       <AssignmentTurnedInIcon style={{ fontSize: 40 }} /><br/> Finalizados
      </li>
	  <li
        className={page === 'Cancelados' ? 'active' : ''}
        onClick={() => changePage('Cancelados')}
      >
       <CancelIcon style={{ fontSize: 40 }} /><br/> Cancelados
      </li>
	  <li
        className={page === 'Suspensos' ? 'active' : ''}
        onClick={() => changePage('Suspensos')}
      >
       <PauseCircleFilledIcon style={{ fontSize: 40 }} /><br/> Produtos Suspensos
      </li>
	  <li
        
        onClick={handleLogout}
      >
       <ExitToAppIcon style={{ fontSize: 40 }} /><br/> Sair
      </li>

    </Container>
  )
}

SideBar.propTypes = {
  page: PropTypes.string,
  changePage: PropTypes.func.isRequired
}

SideBar.defaultProps = {
  page: 'Orders'
}

export default SideBar