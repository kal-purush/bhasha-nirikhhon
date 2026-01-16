import { Match, History, Location } from '@reach/router';
import { IMarkdownRemark } from '../type';

export interface IPageProps extends IGatsbyPrpos, IPageContext {}

export interface IGatsbyPrpos {
  history: History;
  location: Location;
  match: Match<any>;
}

// see: createPage's context parameter in gatsby-node.js
export interface IPageContext {
  pageContext: {
    slug: string;
    previous: IMarkdownRemark;
    next: IMarkdownRemark;
  };
}