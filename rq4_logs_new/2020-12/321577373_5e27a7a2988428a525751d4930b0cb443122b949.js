const { QRCodeSVG } = require("@cheprasov/qrcode");
const fs = require("fs");
const sharp = require("sharp");
const fetch = require("node-fetch");

const generateQRCodeDataURL = async (
  qrCode,
  imageUrl = "https://www.logoground.com/uploads8/dv8y2020107442020-04-124256584Puppy%20Logo.jpg"
) => {
  // Generate QR code as SVG
  const qrSVG = new QRCodeSVG(qrCode, { level: "Q" });
  const qrSVGString = qrSVG.toString();

  // Fetch remote logo and convert to buffer
  const response = await fetch(imageUrl);
  if (!response.ok) {
    throw new Error(
      `Error fetching imageUrl for QR code: ${response.statusText}`
    );
  }
  const imageBuffer = await response.buffer();
  const imageBufferResized = await new Promise((resolve, reject) => {
    sharp(imageBuffer)
      .resize(100, 100)
      .toBuffer((err, buffer) => {
        if (err) {
          reject(err);
        }
        resolve(buffer);
      });
  });

  // Overlay logo on QR code, then convert QR code into PNG buffer
  const qrBuffer = await new Promise((resolve, reject) => {
    sharp(Buffer.from(qrSVGString), { density: 800 })
      .png()
      .resize(500)
      .composite([{ input: imageBufferResized }])
      .toBuffer((err, buffer) => {
        if (err) {
          reject(err);
        }
        resolve(buffer);
      });
  });

  // Write QR code to HTML file
  fs.writeFileSync(
    "./test_qr.html",
    `<html>
      <body>
        <img src="data:image/png;base64,${qrBuffer.toString("base64")}"/>
      </body>
    </html>`
  );
};

generateQRCodeDataURL(
  JSON.stringify({
    data: "data",
    expiry: 1607678370000,
  })
).then(() => {
  console.log(
    "Generated QR code as PNG data URL. Please open test_qr.html in browser to see it."
  );
});