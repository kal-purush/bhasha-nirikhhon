import { MediaService }from "./media.service";
import {  ExhibitionService } from "./exhibition.service";
import { Media } from "../models/Media";
import { Exhibition } from "../models/Exhibition";
import { Banner } from "../models/Banner";

const mediaService = new MediaService();
const exhibitionService = new ExhibitionService();

export class BannerService {

  public findByPkWithSrc = async(bannerId: string | number) => {
    const banner = await Banner.findByPk(bannerId);
    if (banner.cover_id) {
      const cover: Media | null = await mediaService.findByPkWithSrc(banner.cover_id);
      banner.setDataValue('cover', cover);
    }
    if (banner.cover_mobile_id) {
      const coverMobile: Media | null = await mediaService.findByPkWithSrc(banner.cover_mobile_id);
      banner.setDataValue('cover_mobile', coverMobile);
    }
    if (banner.exhibition_id) {
      const exhibition: Exhibition | null = await exhibitionService.findByPkWithSrc(banner.exhibition_id);
        banner.setDataValue('exhibition', exhibition);
    }
    return banner;
  }

  public findAllWithSrc = async(params) => {
    const banners = await Banner.findAll(params);
    for(const banner of banners) {
      if (banner.cover_id) {
        const cover: Media | null = await mediaService.findByPkWithSrc(banner.cover_id);
        banner.setDataValue('cover', cover);
      }
      if (banner.cover_mobile_id) {
        const coverMobile: Media | null = await mediaService.findByPkWithSrc(banner.cover_mobile_id);
        banner.setDataValue('cover_mobile', coverMobile);
      }
      if (banner.exhibition_id) {
        const exhibition: Exhibition | null = await exhibitionService.findByPkWithSrc(banner.exhibition_id);
          banner.setDataValue('exhibition', exhibition);
      }
    }
    return banners;
  }





}
