import React from "react";
import { Link } from "gatsby";

const RssIcon = ({ link }) => (
  <div className="rss"><Link to={link} title="Subscribe to an RSS feed of this content">RSS</Link></div>
);

export default RssIcon