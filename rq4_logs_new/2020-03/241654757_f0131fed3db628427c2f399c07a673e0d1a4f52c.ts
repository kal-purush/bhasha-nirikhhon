import { Request, Response, response } from "express";
import TagUseCase from "../use-cases/tags/tagUseCase";
import ContentMod from "../utils/ContentMod";

export default class TagController {
  private tagService: TagUseCase;
  private contentMod: ContentMod;

  constructor(taService: TagUseCase) {
    this.tagService = taService;
    this.contentMod = new ContentMod();
  }

  getTag = async (req: Request, res: Response) => {
    try {
      const tag = await this.tagService.BringTag(req.params.id);
      if (!tag) {
        return res.status(200).json({ message: "tag not found" });
      }
      return res.json(tag);
    } catch (error) {
      console.log(error);
      return res.status(500).json({ message: error.message });
    }
  };

  getTags = async (req: Request, res: Response): Promise<Response> => {
    try {
      const tags = await this.tagService.BringAllTags();
      return res.json(tags);
    } catch (error) {
      console.log(error);
      return res.status(500).json({ message: error.message });
    }
  };

  getMyTags = async (req: Request, res: Response): Promise<Response> => {
    try {
      const tags = await this.tagService.BringMyTags(req.params.id);
      return res.json(tags);
    } catch (error) {
      console.log(error);
      return res.status(500).json({ message: error.message });
    }
  };

  createTags = async (req: Request, res: Response) => {
    try {
      const rep = await this.tagService.AddTags(
        req.body,
        res.locals.payload.id,
        req.params.id
      );
      console.log(rep);
      return res.json({ message: "tags created" });
    } catch (error) {
      console.log(error);
      return res.status(500).json({ message: error.message });
    }
  };

  deleteTags = async (req: Request, res: Response): Promise<Response> => {
    try {
      const rep = await this.tagService.RemoveTags(
        req.body,
        res.locals.payload.id,
        req.params.id
      );
      console.log(rep);
      return res.json({ message: "tags deleted" });
    } catch (error) {
      console.log(error);
      return res.status(500).json({ message: error.message });
    }
  };

  deleteTagsTest = async (req: Request, res: Response): Promise<Response> => {
    try {
      const rep = await this.tagService.RemoveTagsTest(req.body);
      console.log(rep);
      return res.json({ message: "tags deleted" });
    } catch (error) {
      console.log(error);
      return res.status(500).json({ message: error.message });
    }
  };
}