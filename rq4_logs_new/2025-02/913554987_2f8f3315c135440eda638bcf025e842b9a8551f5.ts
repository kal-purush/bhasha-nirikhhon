import { deleteImage } from "../cloud/bucketFileManager";
import { ReviewImage } from "../types/interfaces";

export const deleteImages = async (imageList: ReviewImage[]) => {
    await Promise.all(
        imageList.map(async (reviewImage) => {
            const img_url = reviewImage["img_url"];

            await deleteImage(img_url);
        })
    );
};