import React from 'react';
import HomeBannerImage from './HomeBannerImage';

export default class HomeBanner extends React.Component {
	
	render() {
		return (
			<div className="banner-fixed-top">
			<HomeBannerImage />
			</div>
		)
	}
}
