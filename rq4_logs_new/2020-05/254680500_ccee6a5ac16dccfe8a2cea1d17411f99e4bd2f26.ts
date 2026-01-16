import { DesearizableWImage } from './deserializable-w-image';
import { DomSanitizer } from '@angular/platform-browser';

export class Activity implements DesearizableWImage{
    id: string;
    userId: string;
    username: string;
    datetime: Date;
    imageContent: any;
    title: string;


    deserialize(input: any, sanitizer: DomSanitizer): this {
        Object.assign(this, input);
        this.id = input.activityId;

        if (this.imageContent !== null) {
            const objUrl = 'data:image/jpeg;base64,' + this.imageContent;
            this.imageContent = sanitizer.bypassSecurityTrustUrl(objUrl);
        }
        return this;
    }


}