import React from 'react';
import {MapEditor} from './MapEditor.js';
import {SpriteEditor} from './SpriteEditor.js';
import {Home} from './Home.js';
import MaterialIcon from 'material-icons-react';
import './App.css';


const PAGES = {
    '/map_editor':      MapEditor,
    '/sprite_editor':   SpriteEditor,
};
console.log(PAGES);


function Header({currentTab}) {
    return (
        <header>
            <MaterialIcon icon="menu" />
            {currentTab}
        </header>
    );
}

export default function App({pathname}) {
    const SubApp = PAGES[pathname] || Home;
    return (
        <div className="app">
            <Header 
                currentTab={SubApp.title}
            />
            <SubApp />
        </div>
    );
}