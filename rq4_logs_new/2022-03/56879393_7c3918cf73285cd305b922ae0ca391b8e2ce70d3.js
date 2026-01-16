import React from 'react';
import ReactPdf from '@syed-ahmed/docusaurus-plugin-react-pdf/lib/theme/ReactPdf';

export default function DisplayPdf({pdf}) {
	const [numPages, setNumPages] = React.useState(null);
  	const [pageNumber, setPageNumber] = React.useState(1);
	return (
		<>
		<div >
		  <a href={pdf} className="button button--primary" >Download PDF</a>
		</div>
		<div className="card-demo-container">
		  <div className="shadow--md">
		  <ReactPdf pdf={pdf}
		  	numPages={numPages}
			setNumPages={setNumPages}
			pageNumber={pageNumber}
			setPageNumber={setPageNumber}/>
		  </div>
		</div>
		<nav className="pagination-nav">
		  <div className="pagination-nav__item">
			<button
			  className="pagination-nav__link"
			  type="button"
			  disabled={pageNumber <= 1}
			  onClick={() => setPageNumber(p => Math.max(1, p - 1))}
			>
			  <div className="pagination-nav__sublabel">Previous</div>
			  <div className="pagination-nav__label"></div>
			</button>
  
		  </div>
		  <div className="pagination-nav__item pagination-nav__item--next">
			<button
			  className="pagination-nav__link"
			  type="button"
			  disabled={pageNumber >= numPages}
			  onClick={() => setPageNumber(p => Math.min(numPages, p + 1))}
			>
			  <div className="pagination-nav__sublabel">Next</div>
			  <div className="pagination-nav__label">Page {pageNumber || (numPages ? 1 : '--')} of {numPages || '--'}</div>
			</button>
		  </div>
		</nav>
	  </>
	);
  }