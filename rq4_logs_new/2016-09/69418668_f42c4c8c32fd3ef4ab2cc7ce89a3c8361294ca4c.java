package weesner.taxfetcher;

import android.content.Context;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by alwee on 9/26/2016.
 */

public class Utility {
    // the file names in assets folder to be used in retrieving values
    public static final String MEDICARE = "medicare";
    public static final String SOCIAL_SECURITY = "socialSecurity";
    public static final String ALLOWANCES = "allowances";
    public static final String FEDERAL_INCOME_TAX = "federalIncomeTax";

    // English constants for retrieving allowances
    public static final String PERIOD_TYPE_WEEKLY = "Weekly";
    public static final String PERIOD_TYPE_BIWEEKLY = "Biweekly";
    public static final String PERIOD_TYPE_SEMIMONTHLY = "Semimonthly";
    public static final String PERIOD_TYPE_MONTHLY = "Monthly";
    public static final String PERIOD_TYPE_QUARTERLY = "Quarterly";
    public static final String PERIOD_TYPE_SEMIANNUAL = "Semiannual";
    public static final String PERIOD_TYPE_ANNUAL = "Annual";
    public static final String PERIOD_TYPE_DAILY = "Daily";

    // constants for married and single for
    public static final String MARITAL_STATUS_SINGLE = "Single";
    public static final String MARITAL_STATUS_MARRIED = "Married";

    /**
     * A helper function to retrieve the JSON file from the assets folder
     *
     * @param context    used to retrieve resources from the assets folder
     * @param fileToLoad name of the JSON file to read from
     * @return the string that is to be passed into the JSONObject
     */
    public static JSONObject loadJSONFromAsset(Context context, String fileToLoad) throws JSONException {
        StringBuilder sb = new StringBuilder();
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(context.getResources().getAssets().open(fileToLoad)));
            String temp;
            while ((temp = br.readLine()) != null) {
                sb.append(temp);
            }

            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return new JSONObject(sb.toString());
    }

    /**
     * function to get Social Security and Medicare tax .json files
     *
     * @param context used to retrieve resources from the assets folder
     * @param type    which file to load use MEDICARE or SOCIAL_SECURITY constants, or it will throw an error
     * @param year    has to be 4 digit format eg: 2016
     * @return the percentage of the fica tax taken out of ones check
     */
    public static double getFica(Context context, String type, String year) {
        double ficaValue = 0;
        if (type.equals(MEDICARE) || type.equals(SOCIAL_SECURITY)) {
            try {
                JSONObject ficaObject = loadJSONFromAsset(context, type + ".json");
                JSONArray ficaItems = ficaObject.getJSONArray(type);
                for (int i = 0; i < ficaItems.length(); i++) {
                    if (ficaItems.get(i).equals(year))
                        ficaValue = ficaItems.getDouble(i);
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        } else {
            throw new IllegalArgumentException("Invalid type. Type can only be: " + MEDICARE + " or " + SOCIAL_SECURITY);
        }
        return ficaValue;
    }

    /**
     * helper function to get medicare.json using only year qualifier
     *
     * @param context used to retrieve resources from the assets folder
     * @param year    has to be 4 digit format eg: 2016
     * @return the percentage of ones check that is taken out by medicare
     */
    public static double getMedicare(Context context, String year) {
        return getFica(context, MEDICARE, year);
    }

    /**
     * helper function to get medicare.json using only year qualifier
     *
     * @param context used to retrieve resources from the assets folder
     * @param year    has to be 4 digit format eg: 2016
     * @return the percentage of ones check that is taken out by medicare
     */
    public static double getMedicare(Context context, int year) {
        return getMedicare(context, String.valueOf(year));
    }

    /**
     * helper function to get socialSecurity.json using only year qualifier
     *
     * @param context used to retrieve resources from the assets folder
     * @param year    has to be 4 digit format eg: 2016
     * @return the percentage of ones check that is taken out by social security
     */
    public static double getSocialSecurity(Context context, String year) {
        return getFica(context, SOCIAL_SECURITY, year);
    }

    /**
     * helper function to get socialSecurity.json using only year qualifier
     *
     * @param context used to retrieve resources from the assets folder
     * @param year    has to be 4 digit format eg: 2016
     * @return the percentage of ones check that is taken out by social security
     */
    public static double getSocialSecurity(Context context, int year) {
        return getSocialSecurity(context, String.valueOf(year));
    }

    /**
     * function to get how much each allowance will cost based on periodType and year
     *
     * @param context    used to retrieve resources from the assets folder
     * @param periodType needs to be equal to one of the English constant provided starting with PERIOD_TYPE_
     * @param year       has to be 4 digit format eg: 2016
     * @return the cost of each allowance
     */
    public static double getAllowanceCost(Context context, String periodType, String year) {
        double allowanceCost = 0;
        if (periodType.equals(PERIOD_TYPE_WEEKLY) || periodType.equals(PERIOD_TYPE_BIWEEKLY) || periodType.equals(PERIOD_TYPE_SEMIMONTHLY) ||
                periodType.equals(PERIOD_TYPE_MONTHLY) || periodType.equals(PERIOD_TYPE_QUARTERLY) || periodType.equals(PERIOD_TYPE_SEMIANNUAL) ||
                periodType.equals(PERIOD_TYPE_ANNUAL) || periodType.equals(PERIOD_TYPE_DAILY)) {
            try {
                JSONObject allowancesObject = loadJSONFromAsset(context, ALLOWANCES + ".json");
                JSONArray allowanceItems = allowancesObject.getJSONArray(ALLOWANCES);
                for (int i = 0; i > allowanceItems.length(); i++) {
                    if (allowanceItems.get(i).equals(year)) {
                        JSONArray allowanceTypes = allowanceItems.getJSONArray(i);
                        for (int j = 0; j < allowanceTypes.length(); j++) {
                            if (allowanceTypes.get(j).equals(periodType)) {
                                allowanceCost = allowanceTypes.getDouble(j);
                            }
                        }
                    }
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        } else {
            throw new IllegalArgumentException("Period Type must be equal to one of the English constants provided starting with PERIOD_TYPE_ eg: PERIOD_TYPE_WEEKLY");
        }

        return allowanceCost;
    }

    /**
     * function to get how much each allowance will cost based on periodType and year
     *
     * @param context    used to retrieve resources from the assets folder
     * @param periodType needs to be an English constant provided starting with PERIOD_TYPE_
     * @param year       has to be 4 digit format eg: 2016
     * @return the cost of each allowance
     */
    public static double getAllowanceCost(Context context, String periodType, int year) {
        return getAllowanceCost(context, periodType, String.valueOf(year));
    }

    /**
     * function that gets the total cost of all ones allowances
     *
     * @param context    used to retrieve resources from the assets folder
     * @param periodType needs to be an English constant provided starting with PERIOD_TYPE_
     * @param allowances amount of allowances one has entered, can be 0 - 10 any other number will throw error
     * @param year       has to be 4 digit format eg: 2016
     * @return total cost of all allowances
     */
    public static double getTotalAllowancesCost(Context context, String periodType, int allowances, String year) {
        if (allowances <= 10) {
            return getAllowanceCost(context, periodType, year) * allowances;
        } else {
            throw new IllegalArgumentException("The amount of allowances cannot exceed 10");
        }
    }

    /**
     * function to get the amount of Federal Income Tax to be taken out of ones check
     *
     * @param context       used to retrieve resources from the assets folder
     * @param checkAmount   the gross amount to figure out how much Federal Income Tax will be taken out
     * @param maritalStatus ones marital status needs to be equal to one of the constants starting with MARITAL_STATUS_
     * @param periodType    needs to be an English constant provided starting with PERIOD_TYPE_
     * @param allowances    amount of allowances one has entered, can be 0 - 10 any other number will throw error
     * @param year          has to be 4 digit format eg: 2016
     * @return the amount of Federal Income Tax
     */
    public static double getFederalIncomeTax(Context context, double checkAmount, String maritalStatus, String periodType, int allowances, String year) {
        double fitCost = 0;
        double canBeTaxed = checkAmount - getTotalAllowancesCost(context, periodType, allowances, year);
        try {
            JSONObject federalIncomeTaxObject = loadJSONFromAsset(context, FEDERAL_INCOME_TAX + ".json");
            JSONArray fitItems = federalIncomeTaxObject.getJSONArray(FEDERAL_INCOME_TAX);
            for (int i = 0; i < fitItems.length(); i++) {
                if (fitItems.get(i).equals(year)) {
                    JSONArray fitYearItems = fitItems.getJSONArray(i);
                    for (int j = 0; j < fitYearItems.length(); j++) {
                        if (fitYearItems.get(j).equals(periodType + "_" + maritalStatus)) {
                            boolean needValue = true;
                            for (int k = 0; k < fitYearItems.length(); k++) {
                                try {
                                    if (canBeTaxed <= stringToDouble(fitYearItems.getString(j))) {
                                        if (needValue) {
                                            needValue = false;
                                            JSONObject fitQualifiers = fitYearItems.getJSONObject(j);
                                            double plus = fitQualifiers.getDouble("plus");
                                            double percent = fitQualifiers.getDouble("percent") / 100;
                                            double withheld = fitQualifiers.getDouble("withheld");
                                            fitCost = plus + percent * (canBeTaxed - withheld);
                                        }
                                    }
                                } catch (NumberFormatException nfe) {
                                    if (needValue) {
                                        needValue = false;
                                        if (nfe.getMessage().equals("Invalid double: \"max\"")) {
                                            JSONObject fitQualifiers = fitYearItems.getJSONObject(j);
                                            double plus = fitQualifiers.getDouble("plus");
                                            double percent = fitQualifiers.getDouble("percent") / 100;
                                            double withheld = fitQualifiers.getDouble("withheld");
                                            fitCost = plus + percent * (canBeTaxed - withheld);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return fitCost;
    }

    private static double stringToDouble(String value) {
        if (value.contains(",")) {
            value = value.replace(",", ".");
            return Double.parseDouble(value);
        } else {
            return Double.parseDouble(value);
        }
    }
}