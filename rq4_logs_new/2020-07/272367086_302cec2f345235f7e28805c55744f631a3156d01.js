import React, { useRef, useEffect, useState } from 'react';
import './FooterSection.scss';
import pdf from "../pdfs/Ashton Holgate - CV.pdf";

export function FooterSection(props) {
	const { scrollPosition, windowHeight, borderWidth } = props;
	const WWYWVisibilityMarker = useRef(null);
	const [FooterSectionOffset, setFooterSectionOffset] = useState(0);

	useEffect(() => {
		const bottom = WWYWVisibilityMarker.current.getBoundingClientRect().bottom;
		setFooterSectionOffset(
			(bottom - windowHeight) * 2
		);
	}, [scrollPosition, windowHeight, borderWidth]);

	return (
		<div ref={WWYWVisibilityMarker} className="footer-section">
			<div className="section-wrapper" style={{ transform: `translateY(${FooterSectionOffset}px) translate3d(0,0,0)` }}>
				<p className="email-pre-text">want to get in touch?</p>
				<a className="link" target="_blank" rel="noopener noreferrer" href="mailto:ashton.holgate@gmail.com">send me an email</a>
			</div>
			<div className="section-wrapper" style={{ transform: `translateY(${FooterSectionOffset}px) translate3d(0,0,0)` }}>
				<p className="email-pre-text">or you could</p>
				<a className="link alt" target="_blank" rel="noopener noreferrer" href={pdf}>download my CV</a>
			</div>
		</div>
	);
}