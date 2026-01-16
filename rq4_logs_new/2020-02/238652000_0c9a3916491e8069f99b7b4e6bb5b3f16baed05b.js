var AuthPlugin = {
  setToken: function(token, expiration, user) {
    localStorage.setItem("authToken", token);
    localStorage.setItem("authTokenExpiration", expiration);
    localStorage.setItem("User", user);
  },

  destroyToken: function() {
    localStorage.removeItem("authToken");
    localStorage.removeItem("authTokenExpiration");

    localStorage.removeItem("User");
  },

  getToken: function() {
    var token = localStorage.getItem("authToken");
    var expiration = localStorage.getItem("authTokenExpiration");

    var User = localStorage.getItem("User");

    if (!token || !expiration || !User) return null;

    if (Date.now() > parseInt(expiration)) {
      this.destroyToken();
      return null;
    } else {
      return token;
    }
  },
  getMenus: function() {
    var token = localStorage.getItem("authToken");
    var expiration = localStorage.getItem("authTokenExpiration");
    var User = localStorage.getItem("User");

    if (!token || !expiration || !User) return null;

    if (Date.now() > parseInt(expiration)) {
      this.destroyToken();
      return null;
    } else {
      return Menus;
    }
  },
  loggedIn: function() {
    if (this.getToken()) return true;
    else return false;
  }
};

export default function(Vue) {
  Vue.auth = AuthPlugin;

  Object.defineProperties(Vue.prototype, {
    $auth: {
      get: function() {
        return Vue.auth;
      }
    }
  });
}