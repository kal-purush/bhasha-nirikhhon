import React from 'react';

const IMAGE_SIZE = 30;

const linkStyle = {
	marginRight: 8,
};
const boxStyle = {
	borderRadius: 3,
	display: 'inline-block',
	height: IMAGE_SIZE,
	overflow: 'hidden',
	verticalAlign: 'middle',
	width: IMAGE_SIZE,
};
const imageStyle = {
	display: 'block',
	height: IMAGE_SIZE,
	left: '50%',
	position: 'relative',

	WebkitTransform: 'translateX(-50%)',
	MozTransform: 'translateX(-50%)',
	msTransform: 'translateX(-50%)',
	transform: 'translateX(-50%)',
};
const textStyle = {
	color: '#888',
	display: 'inline-block',
	fontSize: '.8rem',
	marginLeft: 8,
	verticalAlign: 'middle',
};

var CloudinaryImageSummary = React.createClass({
	displayName: 'CloudinaryImageSummary',
	propTypes: {
		image: React.PropTypes.object.isRequired,
		label: React.PropTypes.oneOf(['dimensions', 'publicId']),
	},
	renderLabel () {
		if (!this.props.label) return;

		const { label, image } = this.props;

		let text;

		text = `${image.filename}`;

		return (
			<span style={textStyle}>
				{text}
			</span>
		);
	},
	renderImageThumbnail () {
		if (!this.props.image) return;
		const url = this.props.image.url;
		return <img src={url} style={imageStyle} className="img-load" />;
	},
	render () {
		return (
			<span style={linkStyle}>
				<span style={boxStyle}>
					{this.renderImageThumbnail()}
				</span>
				{this.renderLabel()}
			</span>
		);
	},
});

module.exports = CloudinaryImageSummary;