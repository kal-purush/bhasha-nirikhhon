import React from 'react';

const PageArrows = (props) => {
	return (
		<div className="page-arrow-wrapper flex-container">
			{props.pageNum !== 0 ? (
				<button
					className="flex-box-1"
					onClick={() => {
						props.previousPage();
					}}
				>
					Previous Page
				</button>
			) : null}
			{props.pageNum !== 10 && props.hasNextPage ? (
				<button
					className="flex-box-1"
					onClick={() => {
						props.nextPage();
					}}
				>
					Next Page
				</button>
			) : null}
		</div>
	);
};

export default PageArrows;