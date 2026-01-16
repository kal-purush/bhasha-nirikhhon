import{ Observable } from "rxjs";

import { TestBucketlistPage } from "./test.data.spec";
import { BucketlistModel } from '../models/bucketlist.model';
import { ResponseModel } from '../models/response.model';

export class MockAddBucketlistService {
    response: ResponseModel = {
        message: "Bucketlist added",
        status: "success"
    }

    public addBucketlist(bucketlist: BucketlistModel, path: string, token: string): Observable<ResponseModel>{
        return Observable.of(this.response)
    }

}