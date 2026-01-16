export class PendingBetsFilterDto {
    public skip: number;
    public limit: number | null;
    public exclude_address: string | null;
    public assets: AssetFilter[] | null;
    public liquidation: LiquidationFilter | null;
    public sort_by: object;
}

export class AssetFilter {
    public denom: string;
    public bet_size_from: number | null;
    public bet_size_to: number | null;
}

export class LiquidationFilter {
    public blocks_until_liquidation_from: number | null;
    public blocks_until_liquidation_to: number | null;
}