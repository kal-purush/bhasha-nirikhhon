import React from 'react';
import Nav from 'react-bootstrap/Nav';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import {useSelector} from 'react-redux';

import './Player.scss';

export default function Player() {
    const currentSong = useSelector(state => state.song.currentSong);

    return (
        <Nav as="ul" className="player"> 
            <div className="player__title ml-4">
                {currentSong.title ? (
                    <Row>
                        <Col className="text-center">
                            <img src='/' alt="album coverart" />
                        </Col>
                        <Col>
                            {currentSong.title}
                            <br/>
                            {currentSong.artist.name}
                        </Col>
                    </Row>
                ) : (
                    <></>
                )}
                
            </div>
            <div className="player__controls">2</div>
            <div className="player__volume mr-4">3</div>
        </Nav>
    )
}