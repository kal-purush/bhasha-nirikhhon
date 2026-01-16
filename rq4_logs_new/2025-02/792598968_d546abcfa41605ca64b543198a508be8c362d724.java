package luonq.strategy;

import bean.Total;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import luonq.data.ReadFromDB;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import util.BaseUtils;
import util.Constants;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@Component
public class Strategy41 {

    @Autowired
    private ReadFromDB readFromDB;

    private static List<String> years = Lists.newArrayList("2022", "2023", "2024");

    // 过滤出每天流动性适合的股票（总市值大于20亿，三十天成交量大于9亿，前日收盘价大于10）
    public void filter1() throws Exception {
        BigDecimal standardVol = BigDecimal.valueOf(900000000);
        double standardMarketCap = 2000000000;
        double standardPrice = 10;

        for (String year : years) {
            Map<String, String> fileMap = BaseUtils.getFileMap(Constants.HIS_BASE_PATH + year + "/sharing");

            String path = Constants.HIS_BASE_PATH + "/liquidity/" + year;
            List<String> stockList = Lists.newArrayList();

            for (String stock : fileMap.keySet()) {
                List<String> sharings = BaseUtils.readFile(fileMap.get(stock));
                Map<String/* date */, String> sharingMap = Maps.newHashMap();
                for (String sharing : sharings) {
                    String[] split = sharing.split("\t");
                    sharingMap.put(split[0], sharing);
                }

                List<Total> totals = readFromDB.getCodeDate(year, stock, "asc");

                BigDecimal vol = BigDecimal.ZERO;
                for (int i = 0; i < totals.size(); i++) {
                    Total total = totals.get(i);
                    vol.add(total.getVolume());

                    if (i < 30) {
                        continue;
                    }

                    if (vol.compareTo(standardVol) > 0) {
                        String sharing = sharingMap.get(total.getDate());
                        String[] split = sharing.split("\t");
                        double stockCount = Double.valueOf(split[0]);
                        double marketCap = Double.valueOf(split[1]);
                        double price = marketCap / stockCount;

                        if (marketCap > standardMarketCap && price > standardPrice) {

                        }
                    }
                }
            }
        }
    }
}