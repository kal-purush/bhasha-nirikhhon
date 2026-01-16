package com.example.spoot_taxi_front.adapters;

import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import com.bumptech.glide.Glide;
import com.example.spoot_taxi_front.R;
import com.example.spoot_taxi_front.models.ChatMessage;
import com.example.spoot_taxi_front.models.ChatRoom;
import com.example.spoot_taxi_front.models.User;
import com.example.spoot_taxi_front.network.dto.RallyInformationDto;

import java.util.ArrayList;
import java.util.List;

public class RallyAdapter extends RecyclerView.Adapter<RallyAdapter.RallyViewHolder> {


    private List<RallyInformationDto.RallyDetailsDto> rallyDetails;

    public RallyAdapter() {
        this.rallyDetails = new ArrayList<>();
    }

    public void setRallyDetails(List<RallyInformationDto.RallyDetailsDto> rallyDetails) {
        this.rallyDetails = rallyDetails;
        notifyDataSetChanged();
    }





    // 아이템 뷰를 생성하고 뷰홀더를 반환하는 메서드
    @NonNull
    @Override
    public RallyViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        // 아이템 뷰를 생성하여 뷰홀더에 바인딩
        View itemView = LayoutInflater.from(parent.getContext()).inflate(R.layout.item_rally, parent, false);
        return new RallyViewHolder(itemView);
    }

    @Override
    public void onBindViewHolder(@NonNull RallyViewHolder holder, int position) {
        // 해당 위치의 채팅방 데이터를 가져와서 뷰에 설정
        RallyInformationDto.RallyDetailsDto rallyDetail = rallyDetails.get(position);
        holder.bind(rallyDetail);
    }


    @Override
    public int getItemCount() {
        return rallyDetails.size();
    }


    // 아이템 뷰의 뷰홀더 클래스
    public class RallyViewHolder extends RecyclerView.ViewHolder {

        // 아이템 뷰의 뷰 요소 선언
        private TextView rallyStartTimeTextView;
        private TextView rallyEndTimeTextView;
        private TextView rallyLocationTextView;
        private TextView rallyScaleTextView;
        private TextView rallyJurisdictionTextView;
        private List<ImageView> profileImageViews;
        public RallyViewHolder(@NonNull View itemView) {
            super(itemView);
            // 아이템 뷰의 뷰 요소 초기화
            rallyStartTimeTextView = itemView.findViewById(R.id.rallyStartTimeTextView);
            rallyEndTimeTextView = itemView.findViewById(R.id.rallyEndTimeTextView);
            rallyLocationTextView = itemView.findViewById(R.id.rallyLocationTextView);
            rallyScaleTextView = itemView.findViewById(R.id.rallyScaleTextView);
            rallyJurisdictionTextView = itemView.findViewById(R.id.rallyJurisdictionTextView);

        }

        // 아이템 뷰에 데이터를 설정하는 메서드
        public void bind(RallyInformationDto.RallyDetailsDto rallyDetail) {
            // 아이템 뷰에 데이터 설정
            rallyStartTimeTextView.setText(rallyDetail.getStartTime().getHour()+"시 " + rallyDetail.getStartTime().getMinute() +"분");
            rallyEndTimeTextView.setText(rallyDetail.getEndTime().getHour()+"시 " + rallyDetail.getEndTime().getMinute() +"분");
            rallyLocationTextView.setText(rallyDetail.getLocation());
            rallyScaleTextView.setText(rallyDetail.getRallyScale());
            rallyJurisdictionTextView.setText(rallyDetail.getJurisdiction());
        }
    }
}