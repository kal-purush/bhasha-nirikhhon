import { Video } from "@youmeet/gql/generated";
import dotenv from "dotenv";
import { v2 as cloudinary } from "cloudinary";
import prisma from "@youmeet/prisma-config/prisma";
dotenv.config();

cloudinary.config({
  cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
  api_key: process.env.CLOUDINARY_API_KEY,
  api_secret: process.env.CLOUDINARY_API_SECRET,
  secure: true,
});

const getUptodateVideos = async (videos: Video[]) => {
  //   let result = [];

  for (let i = 0; i < videos.length; i++) {
    const video = videos[i];
    try {
      const url = `https://res.cloudinary.com/de822mdsy/video/upload/q_auto,vc_auto/v2/${video.file?.public_id}.webm`;

      const result = await fetch(url);

      console.log(result.headers.get("x-cld-error"), "result", result);
      //   if (cloudVideo) {
      //     const asset_id = cloudVideo.asset_id;
      //     const public_id = cloudVideo.public_id;
      //     const width = cloudVideo.width;
      //     const height = cloudVideo.height;
      //     const format = cloudVideo.format;
      //     const created_at = cloudVideo.created_at;
      //     const url = cloudVideo.url;
      //     const secure_url = cloudVideo.secure_url;
      //     const folder = cloudVideo.folder;
      //     const original_filename = cloudVideo.original_filename;

      //     let eager = [];
      //     if (cloudVideo.eager) eager = cloudVideo.eager ?? [];
      //     let duration;
      //     if (cloudVideo.duration) duration = cloudVideo.duration;

      //     const updated = await prisma.videos.update({
      //       where: { id: video.id as string },
      //       data: {
      //         file: {
      //           update: {
      //             asset_id,
      //             public_id,
      //             width,
      //             height,
      //             format,
      //             created_at,
      //             url,
      //             secure_url,
      //             folder,
      //             original_filename,
      //             eager,
      //             duration,
      //           },
      //         },
      //         updatedAt: new Date(),
      //       },
      //     });
      //     result.push(updated);
      //   }
    } catch (err: any) {
      console.log(err, "err");
    }
  }
  //   return result;
};

export default getUptodateVideos;