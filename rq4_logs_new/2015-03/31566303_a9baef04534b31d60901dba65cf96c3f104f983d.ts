import react = require('react');
import style = require('ts-style');

var theme = style.create({
	postList: {
		maxWidth: 600,
		marginTop: 60,
		marginLeft: 'auto',
		marginRight: 'auto',

		entry: {
			title: {
				display: 'block',
				fontWeight: 700,
				fontSize: 24,
				textDecoration: 'none',
				marginBottom: 5
			},
			date: {
				color: 'rgba(0,0,0,0.76)'
			},
			snippet: {
				lineHeight: 1.8
			}
		},

		entrySeparator: {
			borderBottom: '1px solid #ccc',
			paddingBottom: 30,
			marginBottom: 30
		},

		readMoreLink: {
			textDecoration: 'none'
		}
	}
});

export interface PostListEntry {
	title: string;
	date: Date;
	snippet: react.ReactNode;
	slug: string;
}

interface PostListProps {
	posts: PostListEntry[];
}

export class PostList extends react.Component<PostListProps,{}> {
	render() {
		var posts = this.props.posts.map((post, index) => {
			var postStyles = [<{}>theme.postList.entry];
			if (index < this.props.posts.length - 1) {
				postStyles.push(theme.postList.entrySeparator);
			}

			return react.DOM.div(style.mixin(postStyles, {
				key: post.title
			}),
				react.DOM.a(style.mixin(theme.postList.entry.title, {
					href: post.slug
				}), post.title),
				react.DOM.div(style.mixin(theme.postList.entry.date), post.date.toDateString()),
				react.DOM.div(style.mixin(theme.postList.entry.snippet), post.snippet),
				react.DOM.a(style.mixin(theme.postList.readMoreLink, {
					href: post.slug
				}), 'Continue Reading...')
			);
		});

		return react.DOM.div(style.mixin(theme.postList),
			posts
		);
	}
}

export var PostListF = react.createFactory(PostList);
