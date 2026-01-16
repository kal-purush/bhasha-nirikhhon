import React from "react";
import API from "../utils/API";
import {
    Media,
    MediaContent,
    MediaLeft,
    MediaRight,
    LevelLeft,
    LevelItem,
    Icon,
    Image,
    Content,
    Level
} from "bloomer";

class Article extends React.Component {
    componentWillMount() {
        this.setState({
            _id: this.props.article._id,
            title: this.props.article.headline.main,
            date: this.props.article.pub_date,
            url: this.props.article.web_url,
        });
    }

    // Save this article to the db
    saveArticle = () => {
        API.saveArticle(this.state).then(res => {
            console.log("Save response: ", res);
            this.props.updateSavedArticles();
            console.log("Article saved!");
        });
    };

    render() {
        return (
            <Media>
                <MediaContent>
                    <Content>
                        <p>
                            <a href={this.state.url}><strong>{this.state.title}</strong></a>
                            <br />
                            <small>
                                {this.state.author} - {this.state.date}
                            </small>
                            <br />
                        </p>
                    </Content>
                    <Level isMobile>
                        <LevelLeft>
                            <LevelItem href="">
                                <Icon
                                    hasTextColor="danger"
                                    isSize="small"
                                    onClick={this.saveArticle}
                                >
                                    <span
                                        className="fa fa-heart"
                                        aria-hidden="true"
                                    />
                                </Icon>
                            </LevelItem>
                        </LevelLeft>
                    </Level>
                </MediaContent>
                <MediaRight />
            </Media>
        );
    }
}

export default Article;