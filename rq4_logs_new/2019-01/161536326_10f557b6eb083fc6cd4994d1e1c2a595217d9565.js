import React, { useState } from 'react'

export default function VideoDetail(video) {

	if (!video) {
		return (
			<div>
				craze
			</div>
		)
	}
	else {
		return (
			<div>
				{video.snippet.title}
			</div>
		)
	}

}