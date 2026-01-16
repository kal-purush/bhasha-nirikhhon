import { Component, OnInit } from '@angular/core';

@Component({
  selector: 'app-empregado',
  templateUrl: './empregado.component.html',
  styleUrls: ['./empregado.component.scss'],
})
export class EmpregadoComponent implements OnInit {
  public appPages = [
    { title: 'Home', url: '/folder/Inbox', icon: 'home' },
    { title: 'Informações', url: '/folder/Outbox', icon: 'information-circle' },
    { title: 'Currículos', url: '/folder/Favorites', icon: 'reader' },
    { title: 'Configurações', url: '/folder/Archived', icon: 'settings' },
  ];
  constructor() { }

  ngOnInit() {}

}