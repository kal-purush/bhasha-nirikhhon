var React = require("react");
var ReactRouter = require("react-router");

var Link = ReactRouter.Link;

var About = React.createClass( {
  render: function() {
    return (
      <div className="container">
        <h2>About: Everything's an Audiobook</h2>
	<p>
		This web app was created as a partial requirement for Dr. Zappala of BYU's 
		CS30 class by Joshua Reynolds.
	</p>
	<p>
           He is happy to allow you free access to its use. This web app is committed to
	   not interrupting your listening to play annoying ads or blocking and distracting
	   you with visual advertising. Many people are auditory learners and will gain far
	   more by listening than reading. This app is quite capable of reading quite long
        </p>
	<p>
	   The text reading uses the mp3 generation service provided by vozme.com. See their license <a href="http://vozme.com/licenses.php?lang=en">here</a>. This website is not affiliated with Vozme or its affiliates.
	</p>
	<p>	
	   Please direct all comments and suggestions to <code>jeyreyjunk@gmail.com</code>
	</p>
      </div>
    );
  }
});

module.exports = About;

