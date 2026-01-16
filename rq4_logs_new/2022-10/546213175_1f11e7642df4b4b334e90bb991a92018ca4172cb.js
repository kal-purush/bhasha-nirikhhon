import jsPDF from 'jspdf';

export default function printDocument(urls) {
  const pdf = new jsPDF('a4');
  for (const imgData in urls) pdf.addImage(imgData, 'JPEG', 0, 0);
  pdf.save('download.pdf');
}