
import PdfDocument from "pdfkit";
import { generate as random } from "randomstring";
import fs from 'fs'
import { Products } from "../../domain/Products";

export const createPdf = (products: Products[]) => {

    const pdf = new PdfDocument()

    const stream = fs.createWriteStream(`./static/${random(10)}.pdf`)
    pdf.pipe(stream);

    pdf.text('Reporte de productos', { align: "center" })
        .moveDown()

    pdf.text(new Date().toString(), { align: "center" })
        .moveDown()

    pdf.image('./static/images/mango.jpg', 0, 0, {
        width: 100,
        height: 100
    })

    pdf.image('./static/images/mango.jpg', 512, 0, {
        width: 100,
        height: 100
    })


    const tableHeaders = ["Id", "Nombre", "Precio", "Unidades"];
    const columnWidths = [100, 200, 100, 100];
    const startX = pdf.page.margins.left;
    let currentY = pdf.y;

    pdf.fontSize(12).font("Helvetica-Bold");
    tableHeaders.forEach((header, index) => {
        const x = columnWidths.slice(0, index).reduce((a, b) => a + b, 0)
        pdf.text(header, startX + x, currentY, {
            width: columnWidths[index],
            align: "left",
        });
    });
    currentY += 20;

    pdf.fontSize(11).font("Helvetica");

    products.forEach(product => {
        const productValues = [
            product.id,
            product.nombre,
            `$${parseFloat(product.precio).toFixed(2)}`,
            product.cantidad_disponible.toString(),
        ]
        // Dibujando la fila [id, nombre, precio, cantidad]
        productValues.forEach((productValue, index) => {
            const x = columnWidths.slice(0, index).reduce((a, b) => a + b, 0)
            pdf.text(productValue.toString(), startX + x, currentY, {
                width: columnWidths[index],
                align: "left",
            });
        })
        currentY += 20;

        // Mover a una nueva página si se llena
        if (currentY > pdf.page.height - pdf.page.margins.bottom - 30) {
            pdf.addPage();
            currentY = pdf.page.margins.top;
        }


    })

    pdf.end()

    // Callback cuando finaliza el pdf
    stream.on('finish', () => {
        console.log('PDF generado');
    })
}