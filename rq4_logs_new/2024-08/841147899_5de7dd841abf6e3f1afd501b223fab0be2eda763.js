import { jsPDF } from 'jspdf';
import 'jspdf-autotable';

export function downloadPDF() {
    const { jsPDF } = window.jspdf;
    const doc = new jsPDF();
    doc.autoTable({ html: '#libreria' });
    doc.save('libreria.pdf');
}

// Crear un nuevo documento jsPDF
const doc = new jsPDF('p', 'pt', 'a4');

// Configuración de la tabla
doc.autoTable({
  head: [['Columna 1', 'Columna 2', 'Columna 3']], // Encabezados de la tabla
  body: [
    ['Fila 1, Columna 1', 'Fila 1, Columna 2', 'Fila 1, Columna 3'],
    ['Fila 2, Columna 1', 'Fila 2, Columna 2', 'Fila 2, Columna 3'],
    // Agrega más filas aquí
  ],
  // Configurar el salto de página automático
  pageBreak: 'auto', // 'auto' divide la tabla automáticamente en páginas si es necesario
  margin: { top: 10 }, // Margen superior
  styles: { overflow: 'linebreak' }, // Estilo para ajustar el contenido
  columnStyles: {
    0: { cellWidth: 'wrap' }, // Ajuste automático de la anchura de la columna
    1: { cellWidth: 'wrap' },
    2: { cellWidth: 'wrap' }
  },
});

// Guardar el PDF
doc.save('tabla.pdf');