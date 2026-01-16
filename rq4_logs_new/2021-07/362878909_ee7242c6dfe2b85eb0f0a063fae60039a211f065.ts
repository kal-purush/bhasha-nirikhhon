import { Deserializable, Pagination } from '@models';

// PACKAGE
import moment from 'moment';

export class NewsCategory implements Deserializable {
  [x: string]: any;
  code!: number;
  error!: number;
  message!: string;
  pagination!: Pagination;
  data!: NewsCategoryData;

  deserialize(input: any): this {
    Object.assign(this, input);

    this.data = input.data.map((res: any) => new NewsCategoryData().deserialize(res));

    return this;
  }
}

export class NewsCategoryData implements Deserializable {
  [x: string]: any;
  id_news_category!: string;
  name!: string;
  title!: string;
  desc!: string;
  is_active!: boolean;
  created_at?: Date;
  modified_at!: Date;

  deserialize(input: any): this {
    return Object.assign(this, input);
  }

  getCreatedAt() {
    return moment(this.created_at).locale('id').format('DD MMMM YYYY');
  }

  getModifiedAt() {
    return moment(this.modified_at).locale('id').format('DD MMMM YYYY');
  }
}