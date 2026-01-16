import react = require('react');
import style = require('ts-style');

var theme = style.create({
	post: {
		maxWidth: 600,
		paddingTop: 60,
		marginLeft: 'auto',
		marginRight: 'auto',
		fontFamily: 'Ubuntu',
		
		title: {
			color: 'rgba(0,0,0,0.87)',
			fontWeight: 700,
			fontSize: 32,
			marginBottom: 10
		},

		date: {
			fontSize: 14,
			color: '#888'
		},

		content: {
			color: 'rgba(0,0,0,0.76)',
			marginTop: 30,
			marginBottom: 30,
			lineHeight: 1.8
		}
	},

	commentBox: {
		marginTop: 30
	}
});

interface PostProps {
	title: string;
	date: Date;
	children?: react.ReactElement<{}>[];
}

interface DisqusProps {
	shortName: string;
}

class DisqusCommentList extends react.Component<DisqusProps,{}> {
	render() {
		var scriptSrc = 'https://' + this.props.shortName + '.disqus.com/embed.js';
		return react.DOM.div(style.mixin(theme.commentBox),
			react.DOM.div({id: 'disqus_thread'}),
			react.DOM.script({
				src: scriptSrc,
				async: true,
				type: 'text/javascript'
			})
		);
	}
}

var DisqusCommentListF = react.createFactory(DisqusCommentList);

export class Post extends react.Component<PostProps,{}> {
	render() {
		return react.DOM.div(style.mixin(theme.post),
		  react.DOM.div(style.mixin(theme.post.title), this.props.title),
		  react.DOM.div(style.mixin(theme.post.date), this.props.date.toDateString()),
		  react.DOM.div(style.mixin(theme.post.content),
			  this.props.children
		  ),
		  DisqusCommentListF({shortName: 'robertknight'})
		);
	}
}

export var PostF = react.createFactory(Post);