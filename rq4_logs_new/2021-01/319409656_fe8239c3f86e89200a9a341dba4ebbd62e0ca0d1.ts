import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';

@Component({
  selector: 'app-home-lector',
  templateUrl: './home-lector.component.html',
  styleUrls: ['./home-lector.component.css']
})
export class HomeLectorComponent implements OnInit {

  constructor(private router:Router) { }

  ngOnInit(): void {
  }

  logOut() {
		localStorage.removeItem('User-token');
		localStorage.removeItem('Expires-in');
		localStorage.removeItem('Username');
		localStorage.removeItem('User-role');
		localStorage.removeItem('Refresh-token');
		localStorage.removeItem('Cart');
		localStorage.removeItem('Id');
		this.router.navigate(['welcome']);
	}

}