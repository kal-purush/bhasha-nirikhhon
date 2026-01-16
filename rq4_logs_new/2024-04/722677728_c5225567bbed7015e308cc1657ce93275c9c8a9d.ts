import { Component, ElementRef, HostListener, OnDestroy, OnInit, Renderer2 } from '@angular/core';
import { CommonModule } from '@angular/common';
import { Subscription } from 'rxjs';
import { BreakpointObserver, Breakpoints } from '@angular/cdk/layout';

@Component({
  selector: 'async-navbar',
  templateUrl: './navbar.component.html',
  styleUrls: ['./navbar.component.scss'],
  standalone: true,
  imports: [CommonModule]
})
export class NavbarComponent implements OnInit, OnDestroy {
  isMenuOpen = true;
  subscriptions: Subscription[] = [];
  isMobile: boolean = false;

  constructor(
    private el: ElementRef, 
    private renderer: Renderer2,
    private breakpointObserver: BreakpointObserver
  ) {}

  ngOnInit() {
    const mobileBtn = this.el.nativeElement.querySelector('.mobile_btn');
    const icon = mobileBtn.querySelector('i');

    this.subscriptions.push(
      this.breakpointObserver.observe([Breakpoints.Handset]).subscribe(state => {
        this.isMobile = state.matches; // Access 'matches' property directly
        if (state.matches) {
          this.isMobile = false;
          icon.classList.toggle('fa-bars');
        } else {
          this.isMobile = true;
          icon.classList.toggle('fa-close');
        }
      })
    )
  }

  

  @HostListener('document:click', ['$event'])
  onClick(event: MouseEvent) {
    if (!this.el.nativeElement.contains(event.target)) {
      this.isMenuOpen = false;
    }
  }

  toggleMenu() {
    this.isMenuOpen = !this.isMenuOpen;
    this.renderer.setStyle(this.el.nativeElement.querySelector('.main_menu'), 'display', this.isMenuOpen ? 'block' : 'none'); 
  }

  ngOnDestroy() {
    // unsubscription
    this.subscriptions.forEach(subscription => {
      if (subscription) {
        subscription.unsubscribe();
      }
    });
    // Clear the array (optional)
    this.subscriptions.length = 0;
  }
}