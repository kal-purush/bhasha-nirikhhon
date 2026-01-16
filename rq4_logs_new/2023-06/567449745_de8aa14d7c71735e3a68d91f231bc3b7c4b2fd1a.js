import React from "react";

import { Link } from "react-router-dom";

import formatNumber from "../../utils/numberFormatting";

import { ReactComponent as DefaultSubredditIcon } from "../../assets/defaultSubredditIcon.svg";
import "./SearchDropdownOption.css";

import PropTypes from "prop-types";

function SearchDropdownOption({ subreddit }) {
  const formattedSubscribers = formatNumber(subreddit.subscribers);

  return (
    <Link className="sr-option" to={subreddit.url}>
      {subreddit.icon_img ? (
        <img className="sr-option__icon" src={subreddit.icon_img} alt="" />
      ) : (
        <DefaultSubredditIcon className="sr-option__icon" />
      )}
      <div className="sr-option__details">
        <div className="sr-option__name">{subreddit.display_name_prefixed}</div>
        <div className="sr-option__members">{formattedSubscribers} members</div>
      </div>
    </Link>
  );
}

SearchDropdownOption.propTypes = {
  subreddit: PropTypes.object.isRequired
};

export default SearchDropdownOption;