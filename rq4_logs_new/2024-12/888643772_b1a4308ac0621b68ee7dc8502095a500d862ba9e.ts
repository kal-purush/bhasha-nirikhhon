export interface News {
  articles: Article[];
  count: number;
  intro_audio: string;
}

export interface Article {
  audio_summary: string;
  audio_summary_url: any;
  categories: string[];
  date_published: string;
  full_text: any;
  article_id: string;
  image: string | null;
  importance_score: number;
  processing_status: string;
  region: string;
  rss_summary: string;
  single_news_item: boolean;
  source_name: string;
  summary_200: string;
  summary_50: string;
  summary_vector: any;
  title: string;
  type: string;
  url: string;
  sound?: Audio.Sound;
}