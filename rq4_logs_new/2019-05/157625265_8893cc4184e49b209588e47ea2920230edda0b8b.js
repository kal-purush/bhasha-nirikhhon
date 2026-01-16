import React, { Component } from "react";
import Button from "@material-ui/core/Button";
import { connect } from "react-redux";
import { readyToDelete, deleteSound } from "../actions";

const Sound = props => {
  let sound = props.sound;
  let index = props.index;
  return sound[Object.keys(sound)] === "closed" ? (
    <Button
      key={index}
      onClick={() => props.prepareToDelete(Object.keys(sound), index)}
    >
      {Object.keys(sound)}
    </Button>
  ) : (
    <span key={index}>
      <Button onClick={() => props.prepareToDelete(Object.keys(sound), index)}>
        {Object.keys(sound)}
      </Button>
      <Button onClick={() => props.deleteThisSound(index)}>X</Button>
    </span>
  );
};
export default connect(
  null,
  {
    prepareToDelete: (sound, index) => readyToDelete(sound, index),
    deleteThisSound: (sound, index) => deleteSound(sound, index)
  }
)(Sound);