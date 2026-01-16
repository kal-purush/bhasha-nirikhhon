import React from 'react';
import { Comment,Image } from "semantic-ui-react";
import TimeAgo from "javascript-time-ago";
import en from "javascript-time-ago/locale/en";

import "./MessageContent.css";

TimeAgo.locale(en);

const MessageContent = (props) => {
    return <Comment>
        <Comment.Avatar src={props.message.user.avatar} />
        <Comment.Content className={props.ownMessage ? "ownMessage" : null}>
            <Comment.Author as="a">{props.message.user.name}</Comment.Author>
                <Comment.Text>{props.message.content}</Comment.Text>
        </Comment.Content>
    </Comment>
}

export default MessageContent;