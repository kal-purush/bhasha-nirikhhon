import { DesearizableWImage } from './deserializable-w-image';

export class Suggestion implements DesearizableWImage{

    id: string;
    imageContent: any;
    title: string;


    deserialize(input: any, sanitizer: import("@angular/platform-browser").DomSanitizer): this {
        Object.assign(this, input);
        this.imageContent = input.image_content;
        this.id = input.quiz_id;

        if (this.imageContent !== null) {
            const objUrl = 'data:image/jpeg;base64,' + this.imageContent;
            this.imageContent = sanitizer.bypassSecurityTrustUrl(objUrl);
        }
        return this;
    }

}