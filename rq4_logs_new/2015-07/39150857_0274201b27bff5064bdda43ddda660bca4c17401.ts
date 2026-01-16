declare var mailru: any; 

module Infro.SocialApi.Mailru {
    export class MailruSocialApi implements ISocialApi {
        private key: string;

        users: IUsersMethods;
        friends: IFriendsMethods;
        dialog: IDialogMethods;

        constructor(key: string, callback: Function) {
            this.key = key;

            this.init(() => {
                console.log('init callback');

                callback();
            });

            this.users = new MailruUsersMethods(this);
        }

        init(callback: Function): void {
            mailru.loader.require('api',() => {
                mailru.app.init(this.key);

                callback();
            });
        }
    }
}