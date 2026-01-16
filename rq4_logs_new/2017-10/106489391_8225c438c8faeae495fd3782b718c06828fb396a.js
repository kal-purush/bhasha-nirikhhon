'use strict';

var firstAndPike = {
  min_customer: 23,
  max_customer: 65,
  avg_cookies: 6.3,
  arrTime: [],
  arrCookies: [],
  customerNum: function() { // generates a random number of customers per hour
    var avg_customer = Math.floor(Math.random() * (this.max_customer - this.min_customer + 1)) + this.min_customer;
    return avg_customer;
  },
  resultsObj: {},
  cookiesPerHour: function() { // calculates number of cookies purchased per hour
    for(var i = 0; i < 15; i++) {
      var time = i + 6;
      this.arrTime.push(time);
      this.arrCookies.push(Math.round(this.customerNum() * this.avg_cookies));
    }
    return this.arrCookies;
  },
  totalCookies: function() {
    var total = 0;
    for (var i = 0; i < this.arrCookies.length; i++) {
      total += this.arrCookies[i];
    }
    return total;
  }
};

var seaTacAirport = {
  min_customer: 3,
  max_customer: 24,
  avg_cookies: 1.2
};

var seattleCenter = {
  min_customer: 11,
  max_customer: 38,
  avg_cookies: 3.7
};

var capitolHill = {
  min_customer: 20,
  max_customer: 38,
  avg_cookies: 2.3
};

var alki = {
  min_customer: 2,
  max_customer: 16,
  avg_cookies: 4.6
};