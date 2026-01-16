package np.com.naxa.factsnepal.surveys;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;

public class SurveyEarningDetails {
    @SerializedName("survey_name")
    @Expose
    private String survey_name;

    @SerializedName("survey_date")
    @Expose
    private String survey_date;

    @SerializedName("earning_from_survey")
    @Expose
    private String earning_from_survey;

    public SurveyEarningDetails(String survey_name, String survey_date, String earning_from_survey) {
        this.survey_name = survey_name;
        this.survey_date = survey_date;
        this.earning_from_survey = earning_from_survey;
    }

    public String getSurvey_name() {
        return survey_name;
    }

    public void setSurvey_name(String survey_name) {
        this.survey_name = survey_name;
    }

    public String getSurvey_date() {
        return survey_date;
    }

    public void setSurvey_date(String survey_date) {
        this.survey_date = survey_date;
    }

    public String getEarning_from_survey() {
        return earning_from_survey;
    }

    public void setEarning_from_survey(String earning_from_survey) {
        this.earning_from_survey = earning_from_survey;
    }


    @Deprecated
    public static ArrayList<SurveyEarningDetails> getDemoItems() {
        ArrayList<SurveyEarningDetails> list = new ArrayList<>();

        list.add(new SurveyEarningDetails("Ncell survey in customer research", "04-09-2019", "Rs.50"));
        list.add(new SurveyEarningDetails("Ncell survey in Marketing research", "04-01-2019", "Rs.10"));
        list.add(new SurveyEarningDetails("Ntc survey in Internet Status", "04-08-2019", "Rs.60"));
        list.add(new SurveyEarningDetails("Tornado survey in Bara, Parsa District", "04-05-2019", "Rs.40"));
        list.add(new SurveyEarningDetails("Weather status survey", "03-30-2019", "Rs.100"));
        list.add(new SurveyEarningDetails("Transportation Facility survey", "02-09-2019", "Rs.30"));
        list.add(new SurveyEarningDetails("Flood survey", "03-15-2019", "Rs.70"));
        list.add(new SurveyEarningDetails("Ncell survey in customer research", "04-09-2019", "Rs.50"));
        list.add(new SurveyEarningDetails("Ncell survey in Marketing research", "04-01-2019", "Rs.10"));
        list.add(new SurveyEarningDetails("Ntc survey in Internet Status", "04-08-2019", "Rs.60"));
        list.add(new SurveyEarningDetails("Tornado survey in Bara, Parsa District", "04-05-2019", "Rs.40"));
        list.add(new SurveyEarningDetails("Weather status survey", "03-30-2019", "Rs.100"));
        list.add(new SurveyEarningDetails("Transportation Facility survey", "02-09-2019", "Rs.30"));
        list.add(new SurveyEarningDetails("Flood survey", "03-15-2019", "Rs.70"));


        return list;
    }
}