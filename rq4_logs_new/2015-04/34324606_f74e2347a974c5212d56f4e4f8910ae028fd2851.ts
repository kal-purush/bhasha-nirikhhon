// Globals
declare var Random: any;
declare var riot: any;

// Define main class
interface Input {
    value: string;
    type: string;
}

class Loose {
    static config = {
        password: {
            alphabet: 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-.',
            length: 8,
        },
    };

    private tag: any;
    private mt: any;

    constructor(tag) {
        this.tag = tag;

        this.updateState();
    }

    get seeder(): string {
        return `${this.seed.value}--${this.title.value}`;
    }

    get seed(): Input {
        return this.tag.seed;
    }

    get title(): Input {
        return this.tag.title;
    }

    set password(password: string) {
        this.tag.password.value = password;
    }

    get showTitle(): boolean {
        return this.tag.showTitle;
    }

    set showTitle(state: boolean) {
        this.tag.showTitle = state;
    }

    get showPassword(): boolean {
        return this.tag.showPassword;
    }

    set showPassword(state: boolean) {
        this.tag.showPassword = state;
    }

    randomString(alphabet: string = Loose.config.password.alphabet, length: number = Loose.config.password.length): string {
        var str = '';

        while (str.length < length) {
            var index = this.mt() % alphabet.length;
            str += alphabet.charAt(index);
        }

        return str;
    }

    generate(): string {
        this.mt = Random.engines.mt19937();
        this.mt.seed(this.seeder);

        return this.randomString();
    }

    updateFocus(target: any, state: boolean) {
        var passwordMode = target != this.seed || !state;

        this.seed.type = passwordMode ? "password" : "text";
    }

    updateState() {
        this.showTitle = this.seed.value != undefined && this.seed.value != '';
        this.showPassword = this.showTitle && this.title.value != undefined && this.title.value != '';

        if (this.showPassword) {
            this.password = this.generate();
        }
    }
}

this.Loose = Loose;

// Start Riot.js
riot.mount('loose');