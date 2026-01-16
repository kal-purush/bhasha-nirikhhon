import React, { Component } from 'react';
import './app.css';

class App extends Component {
	constructor(props){
		super(props);
	}
	render(){
		return (
			<div className='App'>
                <ul className = 'topNav'>
                    <li><a href="#description">Description</a></li>
                    <li><a href="#project">Project</a></li>
                    <li><a href="#file">File</a></li>
                    <li><a href="#about">About</a></li>
                </ul>
                <div className = 'content'>
                    <h1>create-react-app-cli</h1>
                    <div className = 'currentTime'>{new Date().toString().split('(')[0]}</div>
                    <div className = 'postContent'>
                        <p id = 'description'>Description.</p>
                        <p>AttackAPI provides simple and intuitive programmable interface for composing attack vectors with JavaScript and other client and server related technologies.</p>
                        <p id = 'project'>Project.</p>
                        <p>AttackAPI is standard part of many public and private security related projects supported by GNUCITIZEN and other organizations. The library had many incarnations and not all of them were made public. Simply put, internally we call everything AttackAPI that relats to common set of routines useful for offensive research.</p>
                        <p id = 'file'>File.</p>
                        <p>
                        files/2011/05/hack-road-signs-335x1024.jpg<br />
                        files/2011/05/hack-road-signs-49x150.jpg<br />
                        files/2011/05/hack-road-signs-98x300.jpg<br />
                        files/2011/05/hack-road-signs.jpg<br />
                        files/2010/08/backdoor_task-170x150.png<br />
                        files/2010/08/backdoor_task-300x263.png<br />
                        files/2010/08/backdoor_task.png<br />
                        files/2010/08/cf_dir_traversal_exploited-270x150.png<br />
                        files/2010/08/cf_dir_traversal_exploited-300x166.png<br />
                        files/2010/08/cf_dir_traversal_exploited.png<br />
                        files/2011/05/hack-road-signs-335x1024.jpg<br />
                        files/2011/05/hack-road-signs-49x150.jpg<br />
                        files/2011/05/hack-road-signs-98x300.jpg<br />
                        files/2011/05/hack-road-signs.jpg<br />
                        files/2010/08/backdoor_task-170x150.png<br />
                        files/2010/08/backdoor_task-300x263.png<br />
                        files/2010/08/backdoor_task.png<br />
                        files/2010/08/cf_dir_traversal_exploited-270x150.png<br />
                        files/2010/08/cf_dir_traversal_exploited-300x166.png<br />
                        files/2010/08/cf_dir_traversal_exploited.png<br />
                        files/2011/05/hack-road-signs-335x1024.jpg<br />
                        files/2011/05/hack-road-signs-49x150.jpg<br />
                        files/2011/05/hack-road-signs-98x300.jpg<br />
                        files/2011/05/hack-road-signs.jpg<br />
                        files/2010/08/backdoor_task-170x150.png<br />
                        files/2010/08/backdoor_task-300x263.png<br />
                        files/2010/08/backdoor_task.png<br />
                        files/2010/08/cf_dir_traversal_exploited-270x150.png<br />
                        files/2010/08/cf_dir_traversal_exploited-300x166.png<br />
                        files/2010/08/cf_dir_traversal_exploited.png<br />
                        files/2011/05/hack-road-signs-335x1024.jpg<br />
                        files/2011/05/hack-road-signs-49x150.jpg<br />
                        files/2011/05/hack-road-signs-98x300.jpg<br />
                        files/2011/05/hack-road-signs.jpg<br />
                        files/2010/08/backdoor_task-170x150.png<br />
                        files/2010/08/backdoor_task-300x263.png<br />
                        files/2010/08/backdoor_task.png<br />
                        files/2010/08/cf_dir_traversal_exploited-270x150.png<br />
                        files/2010/08/cf_dir_traversal_exploited-300x166.png<br />
                        files/2010/08/cf_dir_traversal_exploited.png<br />
                        </p>
                        <p id = 'about'>About.</p>
                        <p>AttackAPI is open source (GPLv2) project and as such you should feel free to extend upon it. Because AttackAPI means everything and nothing at the same time, for us this project is mostly a placeholder for future work. However, you can still check SVN tags for previous work and code.</p>
                    </div>
                </div>
            </div>
		)
	}	
}
export default App;