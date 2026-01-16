import { D2ModelSchemas, models } from "./schemas";
import { MetadataPickBase, MetadataPayloadBase } from "../api/metadata";
import { D2ApiDefinitionBase, FilterBase } from "../api/common";
import { D2ApiVersioned, D2ApiOptions } from "../api/d2Api";

export * from "../api/index";
export * from "./schemas";

export interface D2ApiDefinition extends D2ApiDefinitionBase {
    schemas: D2ModelSchemas;
    filter: FilterBase;
}

export type MetadataPick<RootSelector> = MetadataPickBase<D2ApiDefinition, RootSelector>;
export type MetadataPayload = MetadataPayloadBase<D2ModelSchemas>;

export class D2Api extends D2ApiVersioned<D2ApiDefinition> {
    public constructor(options?: D2ApiOptions) {
        super(models, options);
    }
}