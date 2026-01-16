import React from 'react';
import { Header, Image } from 'semantic-ui-react';
import { connect } from 'react-redux';
const CharacterDetail = (props) => {
  if (!props.character) {
    return (
      <div>
        <Header as='h2'>No Character Selected</Header>
      </div>
    );
  }
  return (
    <div className='detail'>
      <Image avatar src={props.character.image} circular size='medium' />
      <Header as='h2'>{props.character.name}</Header>
      <div className='feature'>
        <Header as='h6'>Gender: </Header> {props.character.species}
      </div>
      <div className='feature'>
        <Header as='h6'>Species: </Header> {props.character.species}
      </div>
      <div className='feature'>
        <Header as='h6'>Origin: </Header> {props.character.origin.name}
      </div>
      <div className='feature'>
        <Header as='h6'>Episodes: </Header>
        {props.character.episode.length}
      </div>
    </div>
  );
};
const mapStateToProps = (state) => {
  debugger;
  return {
    character: state.selectedCharacter,
  };
};

export default connect(mapStateToProps)(CharacterDetail);