import axios from "axios";
import { OEmbedResponseDto } from "../dto/oemdeb";


export const getOEmbedInfo = async (
  videoUrl: string
): Promise<
  Pick<
    OEmbedResponseDto,
    "title" | "url" | "thumbnail_url" | "author_name" | "author_url"
  >
> => {
  const res = await axios.get<OEmbedResponseDto>(
    `https://noembed.com/embed?url=${videoUrl}`
  );

  const { title, url, thumbnail_url, author_name, author_url, error } =
    res.data;
  if (error) throw new URIError("Invalid video link");

  return {
    title,
    url,
    thumbnail_url,
    author_name,
    author_url,
  };
};