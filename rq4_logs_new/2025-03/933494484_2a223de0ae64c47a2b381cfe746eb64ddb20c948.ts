import sharp from "sharp";

export async function GET(req: Request) {
  const { searchParams } = new URL(req.url);
  const name = searchParams.get("name");

  if (!name) {
    return new Response("Missing name Parameter", { status: 400 });
  }

  // กำหนดขนาดของภาพ
  const width = 800;
  const height = 600;

  // สร้าง SVG ที่มีข้อความเรียงต่อกัน
  const textSize = 14;
  const textColor = "rgba(211,211,211,0.5)";
  const textSpacingX = 120;
  const textSpacingY = 50;

  let svgText = `<svg width="${width}" height="${height}" xmlns="http://www.w3.org/2000/svg">`;

  for (let y = 0; y < height; y += textSpacingY) {
    for (let x = 0; x < width; x += textSpacingX) {
      svgText += `<text x="${x}" y="${y + textSize}" font-size="${textSize}" fill="${textColor}" font-family="Arial" transform="rotate(-15, ${x}, ${y + textSize})">${name}</text>`;
    }
  }

  svgText += `</svg>`;

  // แปลง SVG เป็น Buffer
  const svgBuffer = Buffer.from(svgText);

  // สร้างภาพด้วย sharp
  const image = await sharp({
    create: {
      width,
      height,
      channels: 4,
      background: { r: 0, g: 0, b: 0, alpha: 0 }, // transparent
    },
  })
    .composite([{ input: svgBuffer, top: 0, left: 0 }])
    .png()
    .toBuffer();

  // แปลงเป็น Base64
  const base64Image = image.toString("base64");

  return new Response(
    JSON.stringify({
      image: `data:image/png;base64,${base64Image}`,
    }),
    {
      headers: {
        "Content-Type": "application/json",
      },
    }
  );
}