package com.learning.movietracker;

import android.annotation.SuppressLint;
import android.app.DatePickerDialog;
import android.graphics.drawable.Drawable;
import android.os.Bundle;
import android.text.Editable;
import android.text.SpannableString;
import android.text.Spanned;
import android.text.TextWatcher;
import android.text.style.RelativeSizeSpan;
import android.util.Log;
import android.view.MotionEvent;
import android.view.View;
import android.widget.AdapterView;
import android.widget.Button;
import android.widget.DatePicker;
import android.widget.ImageView;
import android.widget.ListView;
import android.widget.TextView;

import androidx.annotation.Nullable;
import androidx.appcompat.app.AppCompatActivity;
import androidx.core.content.ContextCompat;
import androidx.databinding.BindingAdapter;
import androidx.databinding.DataBindingUtil;
import androidx.lifecycle.Observer;
import androidx.lifecycle.ViewModelProvider;

import com.bumptech.glide.Glide;
import com.learning.movietracker.adapter.SearchListAdaptor;
import com.learning.movietracker.databinding.AddReviewBinding;
import com.learning.movietracker.model.common.Toolbar;
import com.learning.movietracker.model.searchmovies.MovieResults;
import com.learning.movietracker.model.searchmovies.SearchMovieResult;
import com.learning.movietracker.viewmodel.MainActivityViewModel;

import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class AddReviewActivity extends AppCompatActivity {

    private AddReviewBinding addReviewBinding;
    private MainActivityViewModel viewModel;
    private List<MovieResults> movieResultsList;

    @SuppressLint("ClickableViewAccessibility")
    @Override
    protected void onCreate(@Nullable Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        addReviewBinding = DataBindingUtil.setContentView(this, R.layout.add_review);
        Toolbar toolbar = new Toolbar(getString(R.string.addReview), false);
        addReviewBinding.setToolbar(toolbar);
        viewModel = new ViewModelProvider(this).get(MainActivityViewModel.class);
        initialDisplay();

        addReviewBinding.headerSection.toolbarBacButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                onBackPressed();
            }
        });

        addReviewBinding.searchbox.addTextChangedListener(new TextWatcher() {
            @Override
            public void beforeTextChanged(CharSequence charSequence, int i, int i1, int i2) {
            }

            @Override
            public void onTextChanged(CharSequence charSequence, int start, int before, int count) {
                if (charSequence.length() > 0) {
                    Drawable rightDrwable = ContextCompat.getDrawable(getApplicationContext(), R.drawable.icons8_clear_20);
                    addReviewBinding.searchbox.setCompoundDrawablesWithIntrinsicBounds(null, null, rightDrwable, null);
                    viewModel.searchBarKeyTyped(charSequence.toString());
                    getSearchedMoviesResult();
                } else {
                    Drawable leftDrawable = ContextCompat.getDrawable(getApplicationContext(), R.drawable.icons8_search_20);
                    addReviewBinding.searchbox.setCompoundDrawablesWithIntrinsicBounds(leftDrawable, null, null, null);
                    initialDisplay();
                }
            }

            @Override
            public void afterTextChanged(Editable editable) {
            }
        });

        addReviewBinding.searchbox.setOnTouchListener(new View.OnTouchListener() {
            @Override
            public boolean onTouch(View view, MotionEvent motionEvent) {
                if (motionEvent.getAction() == MotionEvent.ACTION_UP) {
                    Drawable rightDrwable = ContextCompat.getDrawable(getApplicationContext(), R.drawable.icons8_clear_20);
                    if (motionEvent.getRawX() >= (addReviewBinding.searchbox.getRight() - rightDrwable.getBounds().width() - addReviewBinding.searchbox.getCompoundPaddingRight())) {
                        addReviewBinding.searchbox.getText().clear();
                        return true;
                    }
                }
                return false;
            }
        });

        addReviewBinding.searchResultList.setOnItemClickListener(new AdapterView.OnItemClickListener() {
            @Override
            public void onItemClick(AdapterView<?> adapterView, View view, int position, long id) {
                MovieResults movieResult = movieResultsList.get(position);
                Toolbar toolbar = new Toolbar(getString(R.string.iWatched), false);
                addReviewBinding.setToolbar(toolbar);
                switchSearchAndReview();
                setValuesToReviewSection(movieResult);
            }
        });
    }

    private void setValuesToReviewSection(MovieResults movieResult) {
        SpannableString spannableString = new SpannableString(movieResult.getTitle() + " " + movieResult.getReleaseDate().substring(0, 4));
        int spanLength = spannableString.toString().length();
        spannableString.setSpan(new RelativeSizeSpan(0.6f), spanLength - 4, spanLength, Spanned.SPAN_EXCLUSIVE_EXCLUSIVE);
        addReviewBinding.movieName.setText(spannableString);

        String image = "https://image.tmdb.org/t/p/w500/" + movieResult.getPosterPath();
        Glide.with(this).load(image).into(addReviewBinding.movieImage);

        String[] months = {"January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"};
        Date currentDate = new Date();
        Log.i("**Date**", String.valueOf(new Date().getDate()));
        Calendar calendar = Calendar.getInstance();
        addReviewBinding.selectDate.setText(calendar.get(Calendar.DAY_OF_MONTH) + " " + months[calendar.get(Calendar.MONTH)] + " " + calendar.get(Calendar.YEAR));
        addReviewBinding.selectDate.setOnClickListener(v -> showDatePickerDialog(addReviewBinding.selectDate, months));
    }

    private void showDatePickerDialog(TextView dateTextView, String[] months) {
        final Calendar calendar = Calendar.getInstance();
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH);
        int day = calendar.get(Calendar.DAY_OF_MONTH);
        DatePickerDialog datePickerDialog = new DatePickerDialog(this, (datePicker, selectedYear, selectedMonth, selectedDay) -> {
           dateTextView.setText(selectedDay + " " + months[selectedMonth] + " " + selectedYear);
        }, year, month, day);
        datePickerDialog.show();
    }

    private void getSearchedMoviesResult() {
        viewModel.getSearchedMovies().observe(this, new Observer<SearchMovieResult>() {
            @Override
            public void onChanged(SearchMovieResult searchMovieResult) {
                if (searchMovieResult.getResults() != null && !searchMovieResult.getResults().isEmpty()) {
                    ListView searchlistView = addReviewBinding.searchResultList;
                    movieResultsList = searchMovieResult.getResults();
                    SearchListAdaptor searchListAdaptor = new SearchListAdaptor(getApplicationContext(), movieResultsList);
                    searchlistView.setAdapter(searchListAdaptor);
                    addReviewBinding.searchResultArea.setVisibility(View.VISIBLE);
                } else {
                    //display no result message
                }
            }
        });
    }

    private void switchSearchAndReview() {
        addReviewBinding.movieSearchSpace.setVisibility(View.GONE);
        addReviewBinding.movieReviewSpace.setVisibility(View.VISIBLE);
    }

    private void initialDisplay() {
        addReviewBinding.searchResultArea.setVisibility(View.GONE);
        addReviewBinding.movieReviewSpace.setVisibility(View.GONE);
    }

//    public void loadSearchFragment() {
//        FragmentManager fragmentManager = getChildFragmentManager();
//        FragmentTransaction fragmentTransaction = fragmentManager.beginTransaction();
//        fragmentTransaction.replace(R.id.searchFragmentSpace, new SearchResultFragment());
//        fragmentTransaction.commit();
//    }
}