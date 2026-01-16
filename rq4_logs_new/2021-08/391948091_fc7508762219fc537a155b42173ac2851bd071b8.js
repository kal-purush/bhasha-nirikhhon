import React from "react";

import deleteButtonStyles from "./DeleteButton.module.css";

const DeleteButton = (props) => {
  return (
    <form
      className={deleteButtonStyles.deleteButton}
      action={`${props.SERVER_SIDE}/tweets/${props.tweet.id}`}
      method="post"
    >
      <input
        type="hidden"
        name="_method"
        value="Delete"
        data-confirm="Are you sure you want to delete this tweet?"
      />
      <button type="submit">
        <i className="far fa-trash-alt"></i>
      </button>
    </form>
  );
};

export default DeleteButton;