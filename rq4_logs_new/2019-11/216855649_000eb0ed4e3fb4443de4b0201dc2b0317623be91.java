package com.zaffierce.taskplanner;

import androidx.appcompat.app.AppCompatActivity;

import android.os.Bundle;
import android.util.Log;
import android.view.View;

import com.amazonaws.amplify.generated.graphql.CreateTaskMutation;
import com.amazonaws.amplify.generated.graphql.CreateTeamMutation;
import com.amazonaws.amplify.generated.graphql.ListTasksQuery;
import com.amazonaws.amplify.generated.graphql.ListTeamsQuery;
import com.amazonaws.mobile.config.AWSConfiguration;
import com.amazonaws.mobileconnectors.appsync.AWSAppSyncClient;
import com.amazonaws.mobileconnectors.appsync.fetcher.AppSyncResponseFetchers;
import com.apollographql.apollo.GraphQLCall;
import com.apollographql.apollo.api.Response;
import com.apollographql.apollo.exception.ApolloException;

import javax.annotation.Nonnull;

import type.CreateTaskInput;
import type.CreateTeamInput;
import type.TaskState;

public class CollectionsActivity extends AppCompatActivity {

    AWSAppSyncClient awsAppSyncClient;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_collections);

        this.awsAppSyncClient = AWSAppSyncClient.builder()
                .context(getApplicationContext())
                .awsConfiguration(new AWSConfiguration(getApplicationContext()))
                .build();
    }

    public void onButtonPressed(View view) {
        Log.i("zaffierce.task", "clicked the button");
        awsAppSyncClient.query(ListTeamsQuery.builder().build())
                .responseFetcher(AppSyncResponseFetchers.NETWORK_ONLY)
                .enqueue(new GraphQLCall.Callback<ListTeamsQuery.Data>() {
                    @Override
                    public void onResponse(@Nonnull Response<ListTeamsQuery.Data> response) {
                        CreateTaskInput input = CreateTaskInput.builder()
                                .taskTeamId(response.data().listTeams().items().get(0).id())
                                .title("Test")
                                .body("This is a test entry")
                                .taskState(TaskState.NEW)
                                .build();
                        CreateTaskMutation mutation = CreateTaskMutation.builder().input(input).build();
                        awsAppSyncClient.mutate(mutation).enqueue(new GraphQLCall.Callback<CreateTaskMutation.Data>() {
                            @Override
                            public void onResponse(@Nonnull Response<CreateTaskMutation.Data> response) {
                                Log.i("zaffierce.collection", "Added task w/ connections");
                            }

                            @Override
                            public void onFailure(@Nonnull ApolloException e) {
                                Log.e("zaffierce.collection", e.getMessage());
                            }
                        });
                    }

                    @Override
                    public void onFailure(@Nonnull ApolloException e) {

                    }
                });

//        Log.i("zaffierce.task", "onResponse");
//        CreateTeamInput input = CreateTeamInput.builder()
//                .name("teamOne")
//                .build();
//        CreateTeamMutation createTeamMutation = CreateTeamMutation.builder().input(input).build();
//        awsAppSyncClient.mutate(createTeamMutation).enqueue(new GraphQLCall.Callback<CreateTeamMutation.Data>() {
//            @Override
//            public void onResponse(@Nonnull Response<CreateTeamMutation.Data> response) {
//                Log.i("zaffierce.task", "Created successfully");
//            }
//
//            @Override
//            public void onFailure(@Nonnull ApolloException e) {
//                Log.e("zaffierce.task", e.getMessage());
//            }
//        });
    }
}