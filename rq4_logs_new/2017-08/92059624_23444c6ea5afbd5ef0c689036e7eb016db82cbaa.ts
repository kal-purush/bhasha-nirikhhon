import { Component, OnInit} from '@angular/core';
import { Router, NavigationEnd, ActivatedRoute } from '@angular/router';

@Component({
  selector: 'app-navbar',
  templateUrl: './navbar.component.html',
  styleUrls: ['./navbar.component.scss']
})
export class NavbarComponent implements OnInit {
  private show = true;
 
  constructor(private router: Router, private route: ActivatedRoute) {
    this.eval();
  }

  ngOnInit() {
  }

  private eval(){
    this.router.events.subscribe((evt) => {
      if (!(evt instanceof NavigationEnd)) {
        return;
      }

      console.log("Event");
      let url = evt.url.substr(1, evt.url.length - 1).replace('/', '-');
      if (url === 'login' || url === 'register'){
        this.show = false;
      }
    });
  }

}