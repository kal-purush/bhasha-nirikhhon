export class Timeline {

  constructor(
    public $key: string,
    public title: string,
    public description: string,
    public created_date: Date,
    public is_private: boolean
  ){

  }

  static fromJson({$key, title, description, created_date, is_private}): Timeline {
    return new Timeline(
      $key,
      title,
      description,
      created_date,
      is_private
    );
  }

  static fromJsonList(array): Timeline[] {
    return array.map(json => Timeline.fromJson(json));
  }


}