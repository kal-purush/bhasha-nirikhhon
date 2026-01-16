package com.example.potpytown;

import android.content.Intent;
import android.net.Uri;
import android.os.Bundle;
import android.text.TextUtils;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.TextView;
import android.widget.Toast;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.fragment.app.Fragment;

import com.google.android.libraries.places.api.model.Place;

public class DetailPlaceFragment extends Fragment {

    private Place place;

    public static DetailPlaceFragment newInstance(Place place) {
        DetailPlaceFragment fragment = new DetailPlaceFragment();
        Bundle args = new Bundle();
        args.putParcelable("place", place);
        fragment.setArguments(args);
        return fragment;
    }

    @Override
    public void onCreate(@Nullable Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        if (getArguments() != null) {
            place = getArguments().getParcelable("place");
        }
    }

    @Nullable
    @Override
    public View onCreateView(@NonNull LayoutInflater inflater, @Nullable ViewGroup container, @Nullable Bundle savedInstanceState) {
        return inflater.inflate(R.layout.fragment_detailplace, container, false);
    }

    @Override
    public void onViewCreated(@NonNull View view, @Nullable Bundle savedInstanceState) {
        super.onViewCreated(view, savedInstanceState);

        // 레이아웃 요소 초기화
        ImageView imgPlacePhoto = view.findViewById(R.id.img_place);
        TextView txtPlaceName = view.findViewById(R.id.txt_place_name);
        TextView txtPlaceType = view.findViewById(R.id.txt_place_type);
        TextView txtPlaceAddress = view.findViewById(R.id.txt_place_address);
        TextView txtPlacePhone = view.findViewById(R.id.txt_place_phone);
        TextView txtPlaceWebsite = view.findViewById(R.id.txt_place_website);
        ImageView btnSave = view.findViewById(R.id.btn_save);
        ImageView btnMap = view.findViewById(R.id.btn_map);
        ImageView btnClose = view.findViewById(R.id.btn_close);

        btnClose.setOnClickListener(v -> {
            // 프래그먼트 종료
            requireActivity().getSupportFragmentManager().popBackStack();
        });

        // 장소 데이터 설정
        if (place != null) {
            // 장소 이름
            txtPlaceName.setText(place.getDisplayName());

            // 장소 유형
            String placeType = place.getTypes() != null && !place.getTypes().isEmpty() ? place.getTypes().get(0).name() : "정보 없음";
            txtPlaceType.setText(placeType);

            // 장소 주소
            txtPlaceAddress.setText(TextUtils.isEmpty(place.getAddress()) ? "주소 정보 없음" : place.getAddress());

            // 전화번호
            txtPlacePhone.setText(TextUtils.isEmpty(place.getPhoneNumber()) ? "전화번호 정보 없음" : place.getPhoneNumber());

            // 홈페이지 주소
            txtPlaceWebsite.setText(place.getWebsiteUri() != null ? place.getWebsiteUri().toString() : "홈페이지 정보 없음");

            // 장소 사진 (기본 설정 또는 API 연결 필요)
            if (place.getPhotoMetadatas() != null && !place.getPhotoMetadatas().isEmpty()) {
                // 사진 로드 예제 (Glide 사용 가능)
                // Glide.with(this).load(photoUrl).into(imgPlacePhoto);
                imgPlacePhoto.setBackgroundResource(R.drawable.img_place); // 대체용
            } else {
                imgPlacePhoto.setBackgroundResource(R.drawable.img_place); // 기본 이미지
            }

            // 저장 버튼 클릭 이벤트
            btnSave.setOnClickListener(v -> {
                Toast.makeText(getContext(), "장소가 저장되었습니다.", Toast.LENGTH_SHORT).show();
                // TODO: 저장 기능 구현 (예: DB 저장)
            });

            // 지도 버튼 클릭 이벤트
            btnMap.setOnClickListener(v -> {
                if (place.getLatLng() != null) {
                    String geoUri = "geo:" + place.getLatLng().latitude + "," + place.getLatLng().longitude + "?q=" + place.getDisplayName();
                    Intent intent = new Intent(Intent.ACTION_VIEW, Uri.parse(geoUri));
                    intent.setPackage("com.google.android.apps.maps");
                    startActivity(intent);
                } else {
                    Toast.makeText(getContext(), "지도 정보를 사용할 수 없습니다.", Toast.LENGTH_SHORT).show();
                }
            });

            // 홈페이지 클릭 이벤트
            txtPlaceWebsite.setOnClickListener(v -> {
                if (place.getWebsiteUri() != null) {
                    Intent intent = new Intent(Intent.ACTION_VIEW, place.getWebsiteUri());
                    startActivity(intent);
                } else {
                    Toast.makeText(getContext(), "홈페이지 정보를 사용할 수 없습니다.", Toast.LENGTH_SHORT).show();
                }
            });
        } else {
            Toast.makeText(getContext(), "장소 정보를 가져올 수 없습니다.", Toast.LENGTH_SHORT).show();
        }
    }
}