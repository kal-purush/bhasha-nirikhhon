package np.com.naxa.factsnepal.bookmarkedfeed;

import android.view.View;
import android.widget.Button;
import android.widget.ImageView;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import np.com.naxa.factsnepal.FactsNepal;
import np.com.naxa.factsnepal.R;
import np.com.naxa.factsnepal.feed.Fact;
import np.com.naxa.factsnepal.notification.FactsNotification;
import np.com.naxa.factsnepal.utils.ImageUtils;

public class BookmarkedFactsVH extends RecyclerView.ViewHolder {
    private TextView tvBookmarkedFactsTitle, tvBookmarkedFactsSubTitle;
    private ImageView ivBookmarkedFactsImage;
    private Button btnFactUnBookmarked, btnViewDetailsBookmarkedFact;
    private View rootLayout;
    private BookmarkedFactsClickListner bookmarkedFactsClickListner;


    public BookmarkedFactsVH(@NonNull View itemView) {
        super(itemView);
        tvBookmarkedFactsTitle = itemView.findViewById(R.id.tv_bookmarked_facts_title);
        tvBookmarkedFactsSubTitle = itemView.findViewById(R.id.tv_bookmarked_facts_sub_title);
        ivBookmarkedFactsImage = itemView.findViewById(R.id.iv_bookmarked_facts_image);
        btnFactUnBookmarked = itemView.findViewById(R.id.btn_fact_un_bookmarked);
        btnViewDetailsBookmarkedFact = itemView.findViewById(R.id.btn_view_bookmarked_fact_details);
        rootLayout = itemView.findViewById(R.id.root_layout);

    }

    void bindView(Fact fact, BookmarkedFactsClickListner bookmarkedFactsClickListner) {
        this.bookmarkedFactsClickListner = bookmarkedFactsClickListner;

        tvBookmarkedFactsTitle.setText(fact.getTitle());
        tvBookmarkedFactsSubTitle.setText(fact.getCategoryName());

        ImageUtils.loadRemoteImage(FactsNepal.getInstance(), fact.getImagePath()).into(ivBookmarkedFactsImage);

//        rootLayout.setEnabled(factsNotification.isRead());
        btnFactUnBookmarked.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                bookmarkedFactsClickListner.unBookmarkedListner(fact);
            }
        });


        btnViewDetailsBookmarkedFact.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                bookmarkedFactsClickListner.viewBookmarkedListner(fact);
            }
        });
    }


}