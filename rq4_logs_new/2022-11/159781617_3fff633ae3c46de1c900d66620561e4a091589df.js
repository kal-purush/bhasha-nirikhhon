/* eslint-disable */
import React, { Component } from "react";
import axios from "axios";
import { FaSpinner } from "react-icons/fa";
import { XMLParser } from "fast-xml-parser";

const CHANNEL_ID = "UCGJStzEWZVyvZaX0zK4PA9Q";
const parser = new XMLParser({ ignoreAttributes: false });
const corsProxy = "https://cors.newfrontdoor.org/api/cors?url=";
const feedURL = `https://www.youtube.com/feeds/videos.xml?channel_id=${CHANNEL_ID}`;

const withProxy = async () => {
	try {
		const result = await axios.get(`${corsProxy}${feedURL}`);
		const jsonObj = parser.parse(result.data);
		return jsonObj.feed;
	} catch (e) {
		console.error(e);
		return null;
	}
};

class LatestService extends Component {
	constructor() {
		super();
		this.state = { latestVideo: null, error: null };
	}

	componentDidMount() {
		withProxy().then((data) => {
			if (data) this.setState({ latestVideo: data.entry[0], error: false });
			else {
				this.setState({ error: true });
			}
		});
	}
	render() {
		let VideoDetails = () => {
			const latestVideo = this.state.latestVideo;
			const videoId = latestVideo["yt:videoId"];
			const thumbnail = latestVideo["media:group"]["media:thumbnail"]["@_url"];
			const title = latestVideo.title;
			return (
				<div key={_.uniqueId()} className="content">
					<div className="view view-latest-sermon view-id-latest_sermon view-display-id-block">
						<div className="view-content">
							<div className="views-row views-row-1 views-row-odd views-row-first views-row-last">
								<div className="views-field views-field-field-front-page-thumbnail">
									<div className="field-content">
										<a href={`https://www.youtube.com/watch?v=${videoId}`}>
											<img src={thumbnail} alt="" className="img img-responsive" />
										</a>
									</div>
									<br />
									<span className="field-content">
										You can find a link to our latest service on YouTube{" "}
										<a href={`https://www.youtube.com/watch?v=${videoId}`}>here</a>
									</span>
								</div>
							</div>
						</div>
					</div>
				</div>
			);
		};

		return (
			<section>
				<div className="col-md-4 col-xs-12">
					{" "}
					<div className="region region-content-2-1">
						<div className="block block-views">
							<h2 className="header-custom-color">Latest Service</h2>
							{this.state.latestVideo ? (
								<VideoDetails />
							) : this.state.error ? (
								<div>
									<em>Error loading the most recent service.</em>
								</div>
							) : (
								<FaSpinner />
							)}
							{!this.state.latestVideo && (
								<div className="margin-top-20">
									<p>
										Past services can be found on our{" "}
										<a href="https://www.youtube.com/channel/UCGJStzEWZVyvZaX0zK4PA9Q">YouTube channel</a>
									</p>
								</div>
							)}
						</div>
					</div>
				</div>
			</section>
		);
	}
}

export default LatestService;