import { Injectable } from '@angular/core';
@Injectable()
export class TreeViewService {
    public postNumber: number;
    setPostNumber(postNumber) {
        this.postNumber = postNumber;
    }
    getPostNumber() {
        return this.postNumber;
    }
}