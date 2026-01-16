import React from 'react';
import './profileui.css';
// import ShowFonts from './components/react_fontawesome.js';
import FontAwesome from 'react-fontawesome';


function ProfileUi(props) {
  return /*#__PURE__*/React.createElement("div", {
    className: "prof-container"
  }, /*#__PURE__*/React.createElement("div", {
    className: "box2"
  }, 
  

  React.createElement('table', {className: ''},  
  React.createElement('tr', {className: ''},
  React.createElement('td', {className: 'img-box-style'},
  /*#__PURE__*/React.createElement("img", {
    className: "img-box2",
    src: props.imgUrl,
    alt: "profile-img"
  }),

  ),



  React.createElement('td', {className: 'content-box'},


  React.createElement('table', {className: 'content-box'},
  React.createElement('tr', {className: ''},
 /*#__PURE__*/React.createElement("h2", {
  className: "name"
}, props.name),

  ),
  React.createElement('tr', {className: ''},

  React.createElement('td', {className: 'td-icon'},
/*#__PURE__*/React.createElement("h5", {
  className: "far fa-envelope", name:"envelope",size:"1x"
},
),
),
React.createElement('td', {className: ''},
/*#__PURE__*/React.createElement("h5", {
  className: "des"
}, props.emailAddress),
),

),


React.createElement('tr', {className: ''},

React.createElement('td', {className: 'td-icon'},
/*#__PURE__*/React.createElement("h5", {
  className: "far fa-id-badge", name:"id-badge",size:"1x"
},
),
),

React.createElement('td', {className: ''},
/*#__PURE__*/React.createElement("h5", {
  className: "des"
}, props.address),
),

),



React.createElement('tr', {className: ''},
React.createElement('td', {className: 'td-icon'},


/*#__PURE__*/React.createElement("h5", {
  className: "fas fa-phone", name:"phone",size:"1x"
},
),

),
React.createElement('td', {className: ''},
/*#__PURE__*/React.createElement("h5", {
  className: "des"
}, props.phoneNumber),

),

),


React.createElement('tr', {className: ''},
/*#__PURE__*/React.createElement("h5", {
  className: "fas fa-globe", name:"globe",size:"1x"
},
//  /*#__PURE__*/React.createElement("a", {
//   href: props.website
// },props.website),
 /*#__PURE__*/React.createElement("a", {
  href: props.website
},props.website)
),
),






  
),

),
),
),


  ));
}








export default ProfileUi;