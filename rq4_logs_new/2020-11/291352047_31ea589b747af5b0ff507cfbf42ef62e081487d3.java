package com.operationcodify.cavoid.activities;

import androidx.annotation.Nullable;

import com.operationcodify.cavoid.database.LocationDao;

import org.json.JSONException;
import org.json.JSONObject;

public class ParsedPastLocationReport implements Comparable<ParsedPastLocationReport>{
    public String activeCasesEst;
    public String caseFatality;
    public String deathsPer100K;
    public String state;
    public String fips;
    public String casesPer100K;
    public String newCaseNumber;
    public String newDeathNumber;
    public String totalCases;
    public String countyName;
    public String totalDeaths;
    public String reportGenerationDate;

    public String lastReportTimestamp;

    JSONObject response;
    public ParsedPastLocationReport(JSONObject response, LocationDao locDao){
        getInstanceVarsFromResponse(response);
        getLastReportTime(locDao);

    }

    private void getInstanceVarsFromResponse(JSONObject response) {
        activeCasesEst = getStringFromResponse(response, "active_cases_est");
        caseFatality = getStringFromResponse(response, "case_fatality");
        totalDeaths = getStringFromResponse(response, "deaths");
        newDeathNumber = getStringFromResponse(response, "new_daily_deaths");
        deathsPer100K = getStringFromResponse(response, "deaths_per_100k_people");
        totalCases = getStringFromResponse(response, "cases");
        newCaseNumber = getStringFromResponse(response, "new_daily_cases");
        casesPer100K = getStringFromResponse(response, "cases_per_100k_people");
        countyName = getStringFromResponse(response, "county");
        state = getStringFromResponse(response, "state");
        fips = getStringFromResponse(response, "fips");
        reportGenerationDate = getStringFromResponse(response, "report_date");
    }

    private void getLastReportTime(LocationDao locDao){
        if (this.fips == null){
            lastReportTimestamp = null;
        }
        lastReportTimestamp = locDao.getTimeOfLastNotificationFor(this.fips).toString();
    }

    private String getStringFromResponse(JSONObject response, String key){
        String v = null;
        try{
            response.getString(key);
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return v;
    }

    @Override
    public boolean equals(@Nullable Object obj) {
        return obj instanceof ParsedPastLocationReport && ((ParsedPastLocationReport) obj).fips.equals(this.fips);
    }

    @Override
    public int hashCode() {
        return fips.hashCode();
    }

    @Override
    public int compareTo(ParsedPastLocationReport o) {
        if (o.reportGenerationDate == null)
            return 1;
        else if (this.reportGenerationDate == null){
            return -1;
        }
        else return this.reportGenerationDate.compareTo(o.reportGenerationDate);
    }
}