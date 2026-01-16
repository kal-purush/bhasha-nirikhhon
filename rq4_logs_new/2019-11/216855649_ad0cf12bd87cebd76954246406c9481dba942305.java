package com.zaffierce.taskplanner;

import androidx.appcompat.app.AppCompatActivity;
import androidx.recyclerview.widget.LinearLayoutManager;
import androidx.recyclerview.widget.RecyclerView;

import android.os.Bundle;
import android.os.Handler;
import android.os.Looper;
import android.os.Message;
import android.util.Log;
import android.view.View;

import com.amazonaws.amplify.generated.graphql.CreateTaskMutation;
import com.amazonaws.amplify.generated.graphql.CreateTeamMutation;
import com.amazonaws.amplify.generated.graphql.GetTeamQuery;
import com.amazonaws.amplify.generated.graphql.ListTasksQuery;
import com.amazonaws.amplify.generated.graphql.ListTeamsQuery;
import com.amazonaws.mobile.config.AWSConfiguration;
import com.amazonaws.mobileconnectors.appsync.AWSAppSyncClient;
import com.amazonaws.mobileconnectors.appsync.fetcher.AppSyncResponseFetchers;
import com.apollographql.apollo.GraphQLCall;
import com.apollographql.apollo.api.Response;
import com.apollographql.apollo.exception.ApolloException;

import java.util.LinkedList;
import java.util.List;

import javax.annotation.Nonnull;

import type.CreateTaskInput;
import type.CreateTeamInput;
import type.TaskState;

public class PicolasActivity extends AppCompatActivity implements TasksAdapter.OnTaskInteractionListener {

    AWSAppSyncClient awsAppSyncClient;

    RecyclerView picolas;

//    @Override
//    protected void onResume() {
//        super.onResume();
//        awsAppSyncClient.query(ListTeamsQuery.builder().build())
//                .responseFetcher(AppSyncResponseFetchers.NETWORK_ONLY)
//                .enqueue(new GraphQLCall.Callback<ListTeamsQuery.Data>() {
//                    @Override
//                    public void onResponse(@Nonnull Response<ListTeamsQuery.Data> response) {
//
//                    }
//
//                    @Override
//                    public void onFailure(@Nonnull ApolloException e) {
//
//                    }
//                });
//    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        Log.i("veach", "onCreate");
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_collections);

        AWSAppSyncClient client = AWSAppSyncClient.builder()
                .context(getApplicationContext())
                .awsConfiguration(new AWSConfiguration(getApplicationContext()))
                .build();

        picolas = findViewById(R.id.recycler_picolas);
        picolas.setLayoutManager(new LinearLayoutManager(this));

        String id = "95a8d90c-e550-4caf-bb5c-d26c91345022";

        GetTeamQuery query = GetTeamQuery.builder().id(id).build();

        client.query(query).enqueue(new GraphQLCall.Callback<GetTeamQuery.Data>() {
            @Override
            public void onResponse(@Nonnull Response<GetTeamQuery.Data> response) {
                Log.i("veach", "onResponse");
                List<GetTeamQuery.Item> tasks = response.data().getTeam().tasks().items();
                LinkedList<Task> appTasks = new LinkedList<>();
                for (GetTeamQuery.Item task : tasks) {
                    appTasks.add(new Task(task.title(), task.body(), Task.TaskState.NEW));
                }
                Handler handler = new Handler(Looper.getMainLooper()){
                    @Override
                    public void handleMessage(Message message) {
                        picolas.setAdapter(new TasksAdapter(appTasks, PicolasActivity.this));
                    }
                };
                handler.obtainMessage().sendToTarget();
            }

            @Override
            public void onFailure(@Nonnull ApolloException e) {
                Log.e("veach", e.getMessage());

            }
        });

    }

    @Override
    public void taskInterface(Task task) {
        Log.i("veach", "Clicked on task");
    }

//    public void onButtonPressed(View view) {
//        Log.i("zaffierce.task", "clicked the button");
//        awsAppSyncClient.query(ListTeamsQuery.builder().build())
//                .responseFetcher(AppSyncResponseFetchers.NETWORK_ONLY)
//                .enqueue(new GraphQLCall.Callback<ListTeamsQuery.Data>() {
//                    @Override
//                    public void onResponse(@Nonnull Response<ListTeamsQuery.Data> response) {
//                        CreateTaskInput input = CreateTaskInput.builder()
//                                .taskTeamId(response.data().listTeams().items().get(0).id())
//                                .title("Test")
//                                .body("This is a test entry")
//                                .taskState(TaskState.NEW)
//                                .build();
//                        CreateTaskMutation mutation = CreateTaskMutation.builder().input(input).build();
//                        awsAppSyncClient.mutate(mutation).enqueue(new GraphQLCall.Callback<CreateTaskMutation.Data>() {
//                            @Override
//                            public void onResponse(@Nonnull Response<CreateTaskMutation.Data> response) {
//                                Log.i("zaffierce.collection", "Added task w/ connections");
//                            }
//
//                            @Override
//                            public void onFailure(@Nonnull ApolloException e) {
//                                Log.e("zaffierce.collection", e.getMessage());
//                            }
//                        });
//                    }
//
//                    @Override
//                    public void onFailure(@Nonnull ApolloException e) {
//
//                    }
//                });
//    }
}