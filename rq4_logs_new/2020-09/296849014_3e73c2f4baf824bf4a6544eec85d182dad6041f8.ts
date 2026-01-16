export class User {
    login: string;
    name: string;
    avatar_url: string;
    email: string;
    bio: string;
    html_url: string;
    repositories: Array<any>;
    followers: Array<any>;

    constructor(
        login: string = '',
        html_url: string = '',
        name?: string,
        avatar_url?: string,
        email?: string,
        bio?: string
    ) {
        this.login = login;
        this.html_url = html_url;
        this.name = name ? name : 'Not provided';
        this.avatar_url = avatar_url ? avatar_url : 'https://github.githubassets.com/images/modules/logos_page/GitHub-Mark.png';
        this.email = email ? email : 'Not provided';
        this.bio = bio ? bio : 'This user likes to keep a bit of mistery about themself!';
    }
}