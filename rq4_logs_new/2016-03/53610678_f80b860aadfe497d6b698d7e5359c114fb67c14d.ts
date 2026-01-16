let navbarComponent = {
  controller: NavbarController,
  controllerAs: 'vm',
  templateUrl: 'app/components/portal/portal.html',
  
  $routeConfig: [
    {
        path: '/dashboard', 
        name: 'Dashboard', 
        component: 'dashboard', 
        useAsDefault: true
    },
    {
        path: '/user', 
        name: 'User', 
        component: 'user' 
    }
  ]
};

class NavbarController {
    name: string;
  constructor() {
    this.name = 'navbar';
  }
}

export default navbarComponent;