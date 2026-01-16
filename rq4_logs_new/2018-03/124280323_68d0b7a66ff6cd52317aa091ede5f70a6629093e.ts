import { Component, OnInit } from '@angular/core'
import { Router }            from '@angular/router'
import {ObservableMedia}     from '@angular/flex-layout';
// // Servicios
// import { AuthService } from '../../services/auth/auth.service'
// import { DataService } from '../../services/data/data.service'

@Component({
  selector    : 'app-dashboard',
  templateUrl : './dashboard.component.html',
  styleUrls   : ['./dashboard.component.scss']
})

export class DashboardComponent implements OnInit {
  usuario = { nombre: 'John Smith Smith' }
  dashboardMenu = []
  constructor(
    public media       : ObservableMedia,
    public router      : Router,
    // public authService : AuthService,
  ) {}

  addMenuItem (ROUTE) {
    for (let i in this.dashboardMenu) { if (this.dashboardMenu[i].path === ROUTE.path) { return } }
    this.dashboardMenu.push(ROUTE)
  }

  ngOnInit() {
    console.log("Dashboard init")
    // this.usuario.nombre = DataService.getSession().usuario.nombre
    // const SESSION = DataService.getSession()

    this.addMenuItem({ path: '/dashboard/informeasistencia', name: 'Informe de asistencia' })

    // for(let i in SESSION.usuario.roles) {
    //   const rol = SESSION.usuario.roles[i]
    //   if (rol.nombre === 'superadmin') {
    //     this.addMenuItem({ path: '/dashboard/roles', name: 'Roles' })
    //     this.addMenuItem({ path: '/dashboard/usuarios', name: 'Usuarios' })
    //     this.addMenuItem({ path: '/dashboard/clientes', name: 'Clientes' })
    //   }
    //   if (rol.nombre === 'admin') {
    //     this.addMenuItem({ path: '/dashboard/roles', name: 'Roles' })
    //     this.addMenuItem({ path: '/dashboard/usuarios', name: 'Usuarios' })
    //   }
    // }
    console.log("MENU = ", this.dashboardMenu)
  }

  logout() {
    this.router.navigate(['login'])
    // this.authService.logout()
  }
}