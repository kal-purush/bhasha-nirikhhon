import { Banner } from "../models/banner.js"
import ErrorHandler from "../utils/error.js"
import { getDataUri } from "../utils/features.js"
import cloudinary from "cloudinary"

export const newBanner = async(req, res, next) => {

    const file = getDataUri(req.file);
  const myCloud = await cloudinary.v2.uploader.upload(file.content);
  const image = {
    public_id: myCloud.public_id,
    url: myCloud.secure_url,
  };

  await Banner.create({
    images:image
  })

  res.status(200).json({
    success:true,
    message:'Banner created Successfully'
  })




}


export const addNewBanner =  async (req, res, next) => {

    const banner = await Banner.findById(req.params.id)
    if (!banner) return next(new ErrorHandler("Product not found", 404))

    if (!req.file) return next(new ErrorHandler("Please add image", 400));

    const file = getDataUri(req.file);
    const myCloud = await cloudinary.v2.uploader.upload(file.content);
    const image = {
      public_id: myCloud.public_id,
      url: myCloud.secure_url,
    };

    banner.images.push(image)
    await banner.save()

    res.status(200).json({
        success:true,
        message:"Image Added Successfully"
    })

}

 



export const getAllBanner = async (req, res, next) => {

    const Banners = await Banner.find({})
    res.status(200).json({
        success:true,
        Banners,
    })

}