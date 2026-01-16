import { RequestHandler } from "express";
import { IContentHandler } from ".";
import { IContentDto, ICreateContentDto } from "../dto/content";
import { IErrorDto } from "../dto/error";
import { AuthStatus } from "../middleware/jwt";
import { IContentRepository } from "../repositories";

export default class ContentHandler implements IContentHandler {
  private repo: IContentRepository;
  constructor(repo: IContentRepository) {
    this.repo = repo;
  }
  create: RequestHandler<
    {},
    IContentDto | IErrorDto,
    ICreateContentDto,
    undefined,
    AuthStatus
  > = async (req, res) => {
    const { rating, videoUrl, comment } = req.body;

    if (rating > 5 || rating < 0)
      return res.status(400).json({ message: "rating is out of range 0-5" })
        .send;

    const {
      User: { registeredAt, ...userData },
      ...contentData
    } = await this.repo.create(res.locals.user.id, {
      rating,
      videoUrl,
      comment,
      creatorName: "",
      creatorUrl: "",
      thumbnailUrl: `https://i.ytimg.com/vi/${videoUrl.substring(videoUrl.length-11,videoUrl.length)}/hqdefault.jpg`,
      videoTitle: "",
    });

    return res
      .status(201)
      .json({
        ...contentData,
        postedBy: {
          ...userData,
          registeredAt: registeredAt.toISOString(),
        },
      })
      .end();
  };
}