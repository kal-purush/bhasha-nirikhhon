package com.pim.develize.model.response;

import com.pim.develize.model.request.UserInfoModel;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class UserListResponse {
    private List<UserInfoModel> approved;
    private List<UserInfoModel> notApproved;
}