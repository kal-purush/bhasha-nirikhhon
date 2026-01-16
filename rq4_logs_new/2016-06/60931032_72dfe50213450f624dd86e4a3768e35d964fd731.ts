export default [

  '$stormpath',

  ($stormpath: any) => {
    $stormpath.uiRouter({
      loginState: 'login',
      defaultPostLoginState: 'brain'
    });
  }
]