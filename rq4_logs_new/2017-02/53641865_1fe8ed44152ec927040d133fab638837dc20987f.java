package info.santhosh.evlo.common;

import info.santhosh.evlo.data.CommodityContract;

/**
 * Created by santhoshvai on 04/02/2017.
 */

public class DetailCommodityColumnConstants {
    // Specify the columns we need.
    public static final String[] COMMODITY_DETAIL_COLUMNS = {
            CommodityContract.CommodityDataEntry.TABLE_NAME + "." + CommodityContract.CommodityDataEntry._ID,
            CommodityContract.CommodityDataEntry.COLUMN_ARRIVAL_DATE,
            CommodityContract.CommodityDataEntry.COLUMN_MAX_PRICE,
            CommodityContract.CommodityDataEntry.COLUMN_MIN_PRICE,
            CommodityContract.CommodityDataEntry.COLUMN_MODAL_PRICE,
            CommodityContract.CommodityDataEntry.COLUMN_COMMODITY_NAME,
            CommodityContract.CommodityDataEntry.COLUMN_VARIETY,
            CommodityContract.CommodityDataEntry.COLUMN_STATE_NAME,
            CommodityContract.CommodityDataEntry.COLUMN_DISTRICT_NAME,
            CommodityContract.CommodityDataEntry.COLUMN_MARKET_NAME
    };

    // These indices are tied to COMMODITY_DETAIL_COLUMNS.  If COMMODITY_DETAIL_COLUMNS change, these must change.
    public static final int COL_COMMODITY_DETAIL_ID = 0;
    public static final int COL_ARRIVAL_DATE = 1;
    public static final int COL_MAX_PRICE = 2;
    public static final int COL_MIN_PRICE = 3;
    public static final int COL_MODAL_PRICE = 4;
    public static final int COL_COMMODITY_NAME = 5;
    public static final int COL_VARIETY = 6;
    public static final int COL_STATE_NAME = 7;
    public static final int COL_DISTRICT_NAME = 8;
    public static final int COL_MARKET_NAME = 9;
}