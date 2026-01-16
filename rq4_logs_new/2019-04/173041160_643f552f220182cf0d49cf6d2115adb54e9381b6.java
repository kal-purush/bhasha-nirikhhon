package np.com.naxa.factsnepal.surveys;

import android.support.annotation.NonNull;
import android.support.v7.widget.RecyclerView;
import android.view.View;
import android.widget.ImageView;
import android.widget.TextView;

import np.com.naxa.factsnepal.FactsNepal;
import np.com.naxa.factsnepal.R;
import np.com.naxa.factsnepal.utils.ImageUtils;

public class SurveyItemListVH extends RecyclerView.ViewHolder {
    private TextView tvSurveyName, tvSurveyDate;
    ImageView ivSurveyImage;


    public SurveyItemListVH(@NonNull View itemView) {
        super(itemView);
        tvSurveyName = itemView.findViewById(R.id.tv_survey_list_item_title);
        tvSurveyDate = itemView.findViewById(R.id.tv_survey_list_item_date);
        ivSurveyImage = itemView.findViewById(R.id.iv_survey_list_item_image);

    }

    void bindView(SurveyCompany surveyCompany) {
        tvSurveyName.setText(surveyCompany.getTitle());
        tvSurveyDate.setText(surveyCompany.getShortdesc());

        ImageUtils.loadRemoteImage(FactsNepal.getInstance(), surveyCompany.getImage());

    }
}