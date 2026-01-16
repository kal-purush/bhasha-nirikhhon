import ArticleSlider from '@/components/ArticleSlider/ArticleSlider';
import ImageComponent from '@/components/BlogImage/BlogImage';
import ContactGrid from '@/components/ContactGrid/ContactGrid';
import Header from '@/components/Header/Header';
import Hero from '@/components/Hero/Hero';
import LinkComponent from '@/components/Link/Link';
import RichText from '@/components/RichText/RichText';
import BlogListingPage from '@/components/StoryblokPages/BlogListingTemplate/BlogListingPage';
import BlogTemplate from '@/components/StoryblokPages/BlogTemplate/BlogTemplate';
import StartPage from '@/components/StoryblokPages/StartTemplate/StartPage';
import TwoColumn from '@/components/TwoColumn/TwoColumn';

export const components = {
  // Pages
  start_page: StartPage,
  blog_page: BlogTemplate,
  blog_listing_page: BlogListingPage,

  // Modules
  article_slider: ArticleSlider,
  hero: Hero,
  two_column: TwoColumn,
  contact_grid: ContactGrid,

  // Components
  component_blog_image: ImageComponent,
  component_link: LinkComponent,
  component_rich_text: RichText,

  // Global
  header: Header,
};