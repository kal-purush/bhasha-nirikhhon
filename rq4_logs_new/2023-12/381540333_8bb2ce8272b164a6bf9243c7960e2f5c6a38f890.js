document.write(`
<style>

* {
  box-sizing: border-box;
}

/* Control the left side */
.left {
  float:left;
  width:50%;
    padding-top: 90px;
    padding-right: 0px;
    padding-bottom: 50px;
    padding-left: 400px;
}

/* Control the right side */
.right {
  float:right;
  width:50%;
  padding-top: 20px;
  padding-right: 400px;
  padding-bottom: 50px;
  padding-left: 0px;
}

.padding {
  padding: 20px 350px;
}

.cv {
  background-color: #333;   /*Color of the bar */
}

/* If you want the content centered horizontally and vertically */
.centered {
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
  text-align: center;
}
/* Style the image inside the centered container, if needed */
.centered img {
  width: 150px;
  border-radius: 50%;
}

* {
  box-sizing: border-box;
}
body {
  margin: 0;
}
/* Style the header */
.header {
  background-color: #f1f1f1;
  padding: 20px;
  text-align: center;
}
/* Style the top navigation bar */
.topnav {
  overflow: hidden;
  background-color: #333;   /*Color of the bar */
}
/* Style the topnav links */
.topnav a {
  display: inline-block;
  list-style-type: none;
  color: #f2f2f2;           /*Color of the text */
  text-align: center;
  padding: 14px 16px;
  text-decoration: none;
}
/* Change color on hover */
.topnav a:hover {
  background-color: #ddd;       /*Color when hovering (gray) */
  color: black;
}
@media only screen and (max-width:800px) {
  /* For tablets: */
  .main {
    width: 80%;
    padding: 0;
  }
  .right {
    width: 100%;
  }

    .menu, .main, .right {
      padding: 10px;
  }
}
@media only screen and (max-width:500px) {
  /* For mobile phones: */
  .menu, .main, .right {
    width: 100%;
    padding: 10px;
  }
}
</style>

`);