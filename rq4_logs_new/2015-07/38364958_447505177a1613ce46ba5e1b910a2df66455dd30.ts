export module HandleHttpMisc {
    export interface IMIME {
        "Content-Type": string;
    }
    enum HttpCode {
        OK = 200
    }

    interface IHttpMisc {
        MimeType: IMIME;
        HttpCode: number;
    }

    class BaseHttpMisc implements IHttpMisc {
        get MimeType(): IMIME {
            return { 'Content-Type': 'text/plain' }
        }
        get HttpCode(): number {
            return HttpCode.OK;
        }
    }

    export class NormalText extends BaseHttpMisc {

        // use IIFE to initialized static member instead of some fake Class Static Ctor.
        private static _parent: BaseHttpMisc = (() => {
            // do what a Class Static Ctor. should do.
            return new BaseHttpMisc();
        })();

        static get MIME(): IMIME {
            return this._parent.MimeType;
        }

        static get HttpCode(): number {
            return this._parent.HttpCode;
        }
    }

    export class Favicon extends BaseHttpMisc {

        private static _self: Favicon;

        // use a static method invokation to fake Statc Ctor.
        private static invokeStaticCtor = Favicon._initStaticMember();

        /**
         * A fake Class Static Ctor., it must be invoke manually in some where such as other static member's initialization.
         */
        static _initStaticMember() {
            // do what a Class Static Ctor. should do.
            Favicon._self = new Favicon();
            return null;
        }

        static get faviconURL(): string {
            return '/favicon.ico';
        }

        static get MIME(): IMIME {
            var parentMIMEvalue = this._self.MimeType["Content-Type"];

            if (parentMIMEvalue !== "image/x-icon") {
                return { "Content-Type": "image/x-icon" };
            }

            return this._self.MimeType;
        }

        static get HttpCode(): number {
            return Favicon._self.HttpCode;
        }

    }
}