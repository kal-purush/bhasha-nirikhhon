package com.example.movie;

import android.os.Bundle;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.TextView;
import android.widget.Toast;

import androidx.fragment.app.Fragment;

import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;
import retrofit2.converter.gson.GsonConverterFactory;

public class DiaryMemoActivity extends Fragment {

    String genre, title, content, poster_image;
    TextView tvgenre, tvtitle;
    int num;
    Button btnsave;
    String TAG = "Retrofit diarymemo";
    String base = "http://3.36.121.174";

    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container,
                             Bundle savedInstanceState) {
        View view = inflater.inflate(R.layout.diary_memo, container, false);
        //Retrofit 인스턴스 생성
        retrofit2.Retrofit retrofit = new retrofit2.Retrofit.Builder()
                .baseUrl("http://3.36.121.174:8081/")    // baseUrl 등록
                .addConverterFactory(GsonConverterFactory.create())  // Gson 변환기 등록
                .build();

        // 레트로핏 인터페이스 객체 구현
        RetrofitService service = retrofit.create(RetrofitService.class);

        tvgenre = (TextView) view.findViewById(R.id.tvgenre);
        tvtitle = (TextView) view.findViewById(R.id.tvtitle);
        btnsave = (Button) view.findViewById(R.id.btnsave);

        if (getArguments() != null) {
            genre = getArguments().getString("genre"); // 프래그먼트1에서 받아온 값 넣기
            title = getArguments().getString("title"); // 프래그먼트1에서 받아온 값 넣기

            tvgenre.setText("[" + genre + "] ");
            tvtitle.setText(title);

            btnsave.setOnClickListener(new View.OnClickListener() {
                @Override
                public void onClick(View view) {
                    PostResultDiary postresultdiary = new PostResultDiary("test1",num, title, content, poster_image);

                    Call<PostResultDiary> call = service.UpdateDiary(postresultdiary);

                    System.out.println("num="+num);
                    System.out.println("title="+title);
                    System.out.println("poster_image="+poster_image);

                    call.enqueue(new Callback<PostResultDiary>() {
                        @Override
                        public void onResponse(Call<PostResultDiary> call, Response<PostResultDiary> response) {
                            Log.e(TAG, "onResponse");
                            if(response.isSuccessful()){
                                Log.e(TAG, "onResponse success");
                                Toast.makeText(getActivity(), "다이어리에 추가되었습니다", Toast.LENGTH_SHORT).show();

                            }
                            else{
                                // 실패
                                Log.e(TAG, "onResponse fail");
                            }
                        }

                        @Override
                        public void onFailure(Call<PostResultDiary> call, Throwable t) {
                            // 통신 실패
                            Log.e(TAG, "onFailure: " + t.getMessage());
                        }
                    });


                }
            });

        }
        return view;

    }

}