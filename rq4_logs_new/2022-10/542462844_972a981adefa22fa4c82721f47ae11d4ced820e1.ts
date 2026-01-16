import { Category } from './Category.interface';
import { SanitySlug } from './SanitySlug.interface';

export interface BlogPost {
    title: string;
    ingress: string;
    postedDate: string;
    categories: Category[];
    imageUrl: string;
    fireReactions: number;
    surprisedReactions: number;
    mehReactions: number;
    sumReactions: number;
    sumComments: number;
    slug: SanitySlug;
    _id: string;
    _createdAt: string;
  }