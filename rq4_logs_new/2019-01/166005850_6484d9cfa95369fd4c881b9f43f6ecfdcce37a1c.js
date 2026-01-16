"use strict";

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } }

function _createClass(Constructor, protoProps, staticProps) { if (protoProps) _defineProperties(Constructor.prototype, protoProps); if (staticProps) _defineProperties(Constructor, staticProps); return Constructor; }

var getData =
/*#__PURE__*/
function () {
  function getData() {
    _classCallCheck(this, getData);
  }

  _createClass(getData, null, [{
    key: "fetchHotels",
    // Getting Data for hotels
    value: function fetchHotels(callback) {
      var hotelsURL = "http://my-json-server.typicode.com/fly365com/code-challenge/hotels";
      fetch(hotelsURL).then(function (response) {
        return response.json();
      }).then(function (hotels) {
        callback(null, hotels);
      }).catch(function (err) {
        callback(err, null);
      });
    }
  }, {
    key: "fetchHotelInfo",
    value: function fetchHotelInfo(id, callback) {
      var hotelInfoURL = "http://my-json-server.typicode.com/fly365com/code-challenge/hotelDetails/";
      fetch(hotelInfoURL + "/".concat(id)).then(function (response) {
        return response.json();
      }).then(function (hotelInfo) {
        callback(null, hotelInfo);
      }).catch(function (err) {
        callback(null, err);
      });
    }
  }]);

  return getData;
}();
"use strict";

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } }

function _createClass(Constructor, protoProps, staticProps) { if (protoProps) _defineProperties(Constructor.prototype, protoProps); if (staticProps) _defineProperties(Constructor, staticProps); return Constructor; }

var imgGallery =
/*#__PURE__*/
function () {
  function imgGallery() {
    _classCallCheck(this, imgGallery);
  }

  _createClass(imgGallery, null, [{
    key: "createImgGallery",
    value: function createImgGallery(pictures) {
      var _this = this;

      var bigImgs = [];
      var imgCont = document.getElementById('imgGallery'); // Create Big img

      var bigImg = document.createElement('img');
      bigImg.id = 'bigImg';
      bigImg.src = pictures[0].photo;
      imgCont.appendChild(bigImg); // create images for thumbs

      var thumbsBar = document.createElement('div');
      thumbsBar.id = 'thumbsBar';

      for (var thumb = 0; thumb < pictures.length; thumb++) {
        var Img = document.createElement('img');
        Img.src = pictures[thumb].thumbnail;
        thumbsBar.appendChild(Img);
        Img.addEventListener('click', function () {
          _this.changeImg(event, pictures);
        });
      }

      imgCont.appendChild(thumbsBar);
    }
  }, {
    key: "changeImg",
    value: function changeImg(e, pictures) {
      var thumSrc = e.target.src;
      var imgSrc = pictures.find(function (el) {
        return el.thumbnail === thumSrc;
      }).photo;
      console.log(imgSrc);
      bigImg.src = imgSrc;
    } // todo : Make animation fadding image in & out;
    // todo : Adjust the bar for thumbs

  }]);

  return imgGallery;
}();
"use strict";

function _defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } }

function _createClass(Constructor, protoProps, staticProps) { if (protoProps) _defineProperties(Constructor.prototype, protoProps); if (staticProps) _defineProperties(Constructor, staticProps); return Constructor; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

// Hello
var Hotel = function Hotel(id, name, totalReviews, totalScore, pricePerNight, photo) {
  _classCallCheck(this, Hotel);

  this.id = id;
  this.name = name;
  this.totalReviews = totalReviews;
  this.totalScore = totalScore;
  this.pricePerNight = pricePerNight;
  this.photo = photo;
};

var myUI =
/*#__PURE__*/
function () {
  function myUI() {
    _classCallCheck(this, myUI);
  }

  _createClass(myUI, null, [{
    key: "CheckIfData",
    // Passing json for Hotels and builing the UI for it
    value: function CheckIfData() {
      var _this = this;

      getData.fetchHotels(function (error, hotels) {
        if (error) {
          console.log(error);
        } else {
          _this.buildHotels(hotels);
        }
      });
    }
  }, {
    key: "buildHotels",
    value: function buildHotels(hotels) {
      var hotelContainer = document.getElementById('hotels');
      hotels.forEach(function (hotel) {
        var hoteltag = document.createElement('div');
        hoteltag.classList.add('hotels-card');
        hoteltag.classList.add('col');
        hoteltag.innerHTML = "\n                <h1 class=\"hotels-card-name\">".concat(hotel.name, "</h1>\n                <p class=\"hotels-card-review\">").concat(hotel.totalReviews, "</p>\n                <p class=\"hotels-card-score\">").concat(hotel.totalScore, "</p>\n                <p class=\"hotels-card-price\">").concat(hotel.pricePerNight, "</p>\n                <span class=\"hotels-card-more\" data-id=\"").concat(hotel.id, "\" class=\"viewHotel\" onclick=\"myUI.getHotelID(event)\">View Details</span>\n                <img class=\"hotels-card-img\" src=\"").concat(hotel.photo, "\" alt=\"").concat(hotel.name, "\">\n            ");
        hotelContainer.append(hoteltag);
      });
    }
  }, {
    key: "getHotelID",
    value: function getHotelID(e) {
      var _this2 = this;

      e.preventDefault();
      var linkEl = e.target;
      var id = linkEl.dataset.id;

      (function (callback) {
        getData.fetchHotelInfo(id, function (error, HotelInfo) {
          if (!HotelInfo) {
            console.log(error);
            return;
          } else {
            _this2.bulidHotelInfo(HotelInfo);

            _this2.buildHotelReview(HotelInfo);
          }
        });
      })();
    }
  }, {
    key: "clickHotelInfo",
    value: function clickHotelInfo(id) {
      var myHotlInfo = getData.fetchHotelInfo(id);
      console.log(myHotlInfo);
    }
  }, {
    key: "bulidHotelInfo",
    value: function bulidHotelInfo(HotelInfo) {
      document.getElementById('imgGallery').innerHTML = "";
      imgGallery.createImgGallery(HotelInfo.pictures);
    }
  }, {
    key: "buildHotelReview",
    value: function buildHotelReview(HotelInfo) {
      document.getElementById('reviews').innerHTML = "";
      reviews.createReviews(HotelInfo.reviews);
    }
  }]);

  return myUI;
}(); // ** start building the Application


document.addEventListener('DOMContentLoaded', function (event) {
  myUI.CheckIfData();
});
var hotelLinks = document.querySelectorAll('.viewHotel');
"use strict";

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } }

function _createClass(Constructor, protoProps, staticProps) { if (protoProps) _defineProperties(Constructor.prototype, protoProps); if (staticProps) _defineProperties(Constructor, staticProps); return Constructor; }

var reviews =
/*#__PURE__*/
function () {
  function reviews() {
    _classCallCheck(this, reviews);
  }

  _createClass(reviews, null, [{
    key: "createReviews",
    value: function createReviews(reviews) {
      var ReviewsCont = document.getElementById('reviews');
      console.log(reviews);
      reviews.forEach(function (review) {
        var reviewEle = document.createElement('div');
        reviewEle.innerHTML = "\n             <span>".concat(review.score, "</span>\n             <p>").concat(review.review, "</p>\n            ");
        ReviewsCont.append(reviewEle);
      });
    } // todo : create animation when loading the review ele;
    // todo : create paging;

  }]);

  return reviews;
}();
//# sourceMappingURL=app.js.map