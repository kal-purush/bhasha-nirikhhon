import React from "react";
// import { Document, Page } from "react-pdf/dist/esm/entry.webpack";
import resume from "../Contents/resume.pdf";
import { Link } from "react-router-dom";
import "../Stylings/Resume.css";

export default function Resume() {
  return (
    <div className="resume-page">
      <h1 className="page-header">Resume</h1>
      <Link
        className="resume-download-button"
        to={resume}
        target="_blank"
        download
      >
        Download Resume
      </Link>
      <iframe src={resume} title="Resume" className="resume-pdf-frame" />
    </div>
  );
}

// export default function Resume() {
//   const [numPages, setNumPages] = useState(null);
//   const [pageNumber, setPageNumber] = useState(1); //setting 1 to show fisrt page

//   function onDocumentLoadSuccess({ numPages }) {
//     setNumPages(numPages);
//     setPageNumber(1);
//   }

//   function changePage(offset) {
//     setPageNumber((prevPageNumber) => prevPageNumber + offset);
//   }

//   function previousPage() {
//     changePage(-1);
//   }

//   function nextPage() {
//     changePage(1);
//   }

//   return (
//     <div className="resume-page">
//       <h1 className="page-header">Resume</h1>
//       <Link className="certificate-button" to={resume} target="_blank" download>
//         Download Resume
//       </Link>
//       <div className="resume-container">
//         <Document
//           file={resume}
//           options={{ workerSrc: "/pdf.worker.js" }}
//           onLoadSuccess={onDocumentLoadSuccess}
//         >
//           <Page pageNumber={pageNumber} />
//         </Document>
//         <div>
//           <p>
//             Page {pageNumber || (numPages ? 1 : "--")} of {numPages || "--"}
//           </p>
//           <button
//             type="button"
//             disabled={pageNumber <= 1}
//             onClick={previousPage}
//           >
//             Previous
//           </button>
//           <button
//             type="button"
//             disabled={pageNumber >= numPages}
//             onClick={nextPage}
//           >
//             Next
//           </button>
//         </div>
//       </div>
//     </div>
//   );
// }