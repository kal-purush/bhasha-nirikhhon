/// <reference types="cypress" />

import * as indexFuncs from '../pages/indexPage'
import * as dashBoardFuncs from '../pages/dashboardPage'
import * as targets from '../targets/targets'
import * as newRoomFuncs from '../pages/newRoomPage'
import * as roomsFuncs from '../pages/roomsPage'
import * as clientsFuncs from '../pages/clientsPage'
import * as oneClientFuncs from '../pages/oneClientPage'
import * as newClientFuncs from '../pages/newClientPage'
import * as reservationsFuncs from '../pages/reservationsPage'
import * as newReservationsFuncs from '../pages/newReservationsPage'
//test suite
describe('Test suite', function(){
    //före varje test case
    beforeEach(()=>{
        cy.visit(targets.base_url)
        indexFuncs.checkTitleOfIndexPage(cy)
        indexFuncs.performValidLogin(cy, targets.username, targets.password, 'Tester Hotel Overview')
    })

    //efter varje test case
    afterEach(()=>{
        dashBoardFuncs.performLogout(cy, 'Login')
    })

    //test case
    it('Test case 1- Create a room', function(){
        dashBoardFuncs.checkTitleOfDashboardPage(cy)
        dashBoardFuncs.viewRooms(cy, 'Rooms')
        roomsFuncs.clickcreateRoomButton(cy, 'New Room')
        newRoomFuncs.createRoom(cy)
    })

    it('Test case 2- Delete a room', function(){
        dashBoardFuncs.checkTitleOfDashboardPage(cy)
        dashBoardFuncs.viewRooms(cy, 'Rooms')
        roomsFuncs.clickThreePointsDelete(cy)
        //kolla att den försvann??
    })

    it('Test case 3- Edit a clients', function(){
        dashBoardFuncs.checkTitleOfDashboardPage(cy)
        dashBoardFuncs.viewClients(cy, 'Clients')
        clientsFuncs.clickThreePointsEdit(cy)
        oneClientFuncs.editClient(cy, 'Clients')
        //kola att ändrades??
    })

    it('Test case 4- Create a clients', function(){
        dashBoardFuncs.checkTitleOfDashboardPage(cy)
        dashBoardFuncs.viewClients(cy, 'Clients')
        clientsFuncs.clickCreateClientButton(cy, 'New Client')
        newClientFuncs.createClient(cy)
    })

    it('Test case 5- Create a reservation', function(){
        dashBoardFuncs.checkTitleOfDashboardPage(cy)
        dashBoardFuncs.viewReservations(cy, 'Reservations')
        reservationsFuncs.clickCreateReservationButton(cy,'New Reservation')
        newReservationsFuncs.createReservation(cy, 'Reservations')
    })

    it('Test case 6- Deletete a reservation', function(){
        dashBoardFuncs.checkTitleOfDashboardPage(cy)
        dashBoardFuncs.viewReservations(cy, 'Reservations')
        reservationsFuncs.clickthreePointDelete(cy, 'Reservations')
    })
})