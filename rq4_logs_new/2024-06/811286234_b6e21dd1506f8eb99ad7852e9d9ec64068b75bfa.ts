import pdfMake from "pdfmake/build/pdfmake";
import pdfFonts from "pdfmake/build/vfs_fonts";
import * as fs from "fs";
pdfMake.vfs = pdfFonts.pdfMake.vfs;

const outputDir = "pdf/index.pdf";
const docDefinition: any = {
  content: [
    { text: "PDF Title", style: "header" },
    "This is an example of a PDF document generated using pdfmake and TypeScript.",
    { text: "Subheading", style: "subheader" },
    "Here is some more content...",
    {
      layout: "lightHorizontalLines", // optional
      table: {
        // headers are automatically repeated if the table spans over multiple pages
        // you can declare how many rows should be treated as headers
        headerRows: 1,
        widths: ["*", "auto", 100, "*"],

        body: [
          ["First", "Second", "Third", "The last one"],
          ["Value 1", "Value 2", "Value 3", "Value 4"],
          [{ text: "Bold value", bold: true }, "Val 2", "Val 3", "Val 4"],
        ],
      },
    },
  ],
  styles: {
    header: {
      fontSize: 18,
      bold: true,
    },
    subheader: {
      fontSize: 15,
      bold: true,
    },
  },
};

pdfMake.createPdf(docDefinition).getDataUrl((data) => {
  const base64Data: string = data;
  const base64String: string = base64Data.split(";base64,").pop() || "";
  const pdfBuffer: Buffer = Buffer.from(base64String, "base64");

  // Write buffer to a file
  fs.writeFile(outputDir, pdfBuffer, (err: NodeJS.ErrnoException | null) => {
    if (err) {
      console.error("Error:", err);
      return;
    }
    console.log("PDF file has been saved!");
  });
});