import { Model } from "./Model";
import { ObjectManager } from "./ObjectManager";
import { PageResult } from "./PageResult";
import { PageQuery } from "./query";
declare type ValidPropertyType = number | string | boolean | ValidPropertyType[];
declare type QueryParams<T> = {
    [k: string]: ValidPropertyType;
} | Partial<T>;
export declare abstract class AbstractCollectionManager<T extends Model> {
    abstract get collection_url(): string;
    abstract get T(): new () => T;
    page({ query, page, }: {
        query?: QueryParams<T>;
        page?: PageQuery;
    }): Promise<PageResult<T>>;
    get(query: QueryParams<T>): Promise<ObjectManager<T>>;
}
export {};
//# sourceMappingURL=AbstractCollectionManager.d.ts.map