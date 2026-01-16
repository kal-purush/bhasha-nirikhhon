package com.jedikv.simpleconverter.api;

import android.content.Context;

import com.jedikv.simpleconverter.App;
import com.jedikv.simpleconverter.api.responses.YahooCurrencyRate;
import com.jedikv.simpleconverter.api.responses.YahooDataContainer;
import com.jedikv.simpleconverter.busevents.CurrencyUpdateEvent;
import com.jedikv.simpleconverter.dbutils.CurrencyPairDbHelper;
import com.jedikv.simpleconverter.utils.YahooApiUtils;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import converter_db.CurrencyPairEntity;
import hirondelle.date4j.DateTime;
import retrofit.RestAdapter;
import rx.Observable;
import rx.Subscriber;
import rx.android.schedulers.AndroidSchedulers;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.schedulers.Schedulers;
import timber.log.Timber;

/**
 * Created by Kurian on 13/06/2015.
 */
public class YahooCurrencyDownloadService {

    private static final String TAG = YahooCurrencyDownloadService.class.getSimpleName();

    private IYahooCurrencyApi api;
    private RestAdapter restAdapter;

    public YahooCurrencyDownloadService() {
        Timber.tag(TAG);
        restAdapter = YahooCurrencyRestAdapter.getInstance();
        api = restAdapter.create(IYahooCurrencyApi.class);

    }

    public void executeRequest(final Context context, List<String> targetCurrencies, String sourceCurrency) {

        List<String> currencyPair = YahooApiUtils.createReverseFromPairs(targetCurrencies, sourceCurrency);
        String query = YahooApiUtils.generateYQLCurrencyQuery(currencyPair);

        api.getCurrencyPairs(query).map(new Func1<YahooDataContainer, List<CurrencyPairEntity>>() {
            @Override
            public List<CurrencyPairEntity> call(YahooDataContainer yahooDataContainer) {
                try {

                    List<CurrencyPairEntity> list = generateCurrencyPairList(yahooDataContainer.getQuery());

                    return list;
                } catch (ParseException e) {
                    e.printStackTrace();
                    return null;
                }
            }
        }).subscribe(new Subscriber<List<CurrencyPairEntity>>() {
            @Override
            public void onCompleted() {
                Timber.d("Request success!");
                App.getBusInstance().post(new CurrencyUpdateEvent());
            }

            @Override
            public void onError(Throwable e) {
                Timber.e(e, e.getMessage());
            }

            @Override
            public void onNext(List<CurrencyPairEntity> currencyPairEntityList) {
                try {
                    saveCurrencyData(context, currencyPairEntityList);
                } catch (ParseException e) {
                    e.printStackTrace();
                    Timber.e(e, e.getMessage());
                }

            }
        });
    }

    private List<CurrencyPairEntity> generateCurrencyPairList(YahooDataContainer.YahooCurrencyQueryResult result) throws ParseException{

        DateTime timestamp = new DateTime(result.getCreated());
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date date = dateFormat.parse("2013-12-4");

        List<YahooCurrencyRate> rates = result.getResults().getRate();

        List<CurrencyPairEntity> entities = new ArrayList<>();

        for(YahooCurrencyRate rate : rates) {

            CurrencyPairEntity entity = new CurrencyPairEntity();
            entity.setDate(date);
            entity.setPair(rate.getName());
            BigDecimal decimalRate = new BigDecimal(rate.getRate());

            Timber.d("BigDecimalRate: " + decimalRate.toPlainString() + " Rate String: " + rate.getRate());

            //Set the rate to a full integer to prevent any rounding errors from floats/doubles
            entity.setRate(decimalRate.multiply(new BigDecimal(10000)).intValue());
            entities.add(entity);
        }

        return entities;
    }

    /**
     * Save to local database
     * @param result the yahoo currency result
     */
    private void saveCurrencyData(Context context, List<CurrencyPairEntity> currencyPairEntityList) throws ParseException {

        CurrencyPairDbHelper helper = new CurrencyPairDbHelper(context);
        helper.bulkInsertOrUpdate(currencyPairEntityList);

        App.getBusInstance().post(new CurrencyUpdateEvent());
    }

    private IYahooCurrencyApi getApi() {
        return api;
    }
}