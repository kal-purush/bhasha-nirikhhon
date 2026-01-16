package com.teck.githubpeoplesearchtest;

import android.content.Context;
import android.content.Intent;
import android.support.v7.widget.RecyclerView;

import com.google.gson.Gson;
import com.teck.githubpeoplesearchtest.activities.GithubUserDetailActivity;
import com.teck.githubpeoplesearchtest.adapter.GithubUsersListAdapter;
import com.teck.githubpeoplesearchtest.adapter.UserSelectListener;
import com.teck.githubpeoplesearchtest.model.GitHumSearchResponse;
import com.teck.githubpeoplesearchtest.model.GithubUser;
import com.teck.githubpeoplesearchtest.web.ApiControler;

import java.util.ArrayList;

import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;

public class TestClass implements UserSelectListener {

    public void searchFriends(){

//        Intent intent=new Intent(this,GithubUserDetailActivity.class);

      //  val receivedModel=gson.fromJson(receivedModelStr,GithubUser.class));

Gson gson=new Gson();

GithubUser githubUser=gson.fromJson("",GithubUser.class);


        ArrayList<GithubUser> githubUsers=null;
        Context context=null;
        RecyclerView recyclerView=null;

        GithubUsersListAdapter githubUsersListAdapter=new GithubUsersListAdapter(context,githubUsers,TestClass.this);
        recyclerView.setAdapter(githubUsersListAdapter);
    }

    @Override
    public void onUserSelected(GithubUser selectedUser) {

    }
}