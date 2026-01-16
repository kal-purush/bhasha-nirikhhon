import React from "react"; // eslint-disable-line no-unused-vars

const DEFAULT_WIDTH = "100%";
const DEFAULT_MESSAGE = "";

const ProgressBar = (props) => {
	const style = { width: props.width || DEFAULT_WIDTH };

	return (
		<div className="progress">
			<div className="progress-bar progress-bar-info progress-bar-striped active" style={style}>
				<span>{props.message || DEFAULT_MESSAGE}</span>
			</div>
		</div>
	);
};

export default ProgressBar;