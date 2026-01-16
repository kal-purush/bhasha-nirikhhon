import React from 'react';

const NotesList = (props) => {
  let notes = props.notes.map(function(note, index) {
    return (
      <li className="list-group-item" key={index}>{note['.value']}</li>
    );
  });
  return (
    <ul className="list-group">
      {notes}
    </ul>
  );
};

export default NotesList;