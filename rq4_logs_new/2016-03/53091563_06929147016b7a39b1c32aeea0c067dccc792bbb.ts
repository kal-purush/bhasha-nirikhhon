import { get, split} from 'lodash';

export class Profile {
    name: String
    age: Integer
    phone: String
    mail: String
    avatar: String
    description: String
    school: String
    tags: String[]
    job: String

    constructor(json: Object) {
        this.name = get(json, 'name', 'Unknown');
        this.age = get(json, 'age', 0);
        this.phone = get(json, 'phone', 'Unknown');
        this.mail = get(json, 'mail', 'Unknown');
        this.school = get(json, 'school', 'Unknown');
        this.avatar = get(json, 'avatar', '../../img/default_avatar.jpg');
        this.description = get(json, 'description', 'Unknown');
        this.tags = get(json, 'tags', 'Unknown').split(',');
        this.job = get(json, 'job', 'Unknown');
    }
}