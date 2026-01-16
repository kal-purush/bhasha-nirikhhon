import sharp from "sharp";
import axios from "axios";

export async function mergeImages(urls: string[]): Promise<Buffer> {
  const width = 512;
  const height = 512;

  const buffers = await Promise.all(
    urls.map(async (url) => {
      const response = await axios.get(url, { responseType: "arraybuffer" });
      return sharp(response.data).resize(width, height).toBuffer();
    }),
  );

  const composite = buffers.map((buffer) => ({ input: buffer }));

  const mergedImage = await sharp({
    create: {
      width,
      height,
      channels: 4,
      background: { r: 0, g: 0, b: 0, alpha: 0 },
    },
  })
    .composite(composite)
    .png()
    .toBuffer();

  return mergedImage;
}