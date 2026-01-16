package com.example.pulopo.Activity;

import androidx.appcompat.app.AppCompatActivity;
import androidx.appcompat.widget.Toolbar;
import androidx.recyclerview.widget.LinearLayoutManager;
import androidx.recyclerview.widget.RecyclerView;

import android.content.Intent;
import android.net.Uri;
import android.os.Build;
import android.os.Bundle;
import android.os.Handler;
import android.provider.MediaStore;
import android.text.TextUtils;
import android.util.Log;
import android.view.View;
import android.widget.EditText;
import android.widget.ImageView;
import android.widget.Toast;


import com.example.pulopo.Adapter.ChatAdapter;
import com.example.pulopo.R;
import com.example.pulopo.Retrofit.ApiServer;
import com.example.pulopo.Retrofit.RetrofitClient;
import com.example.pulopo.Utils.LocationUtil;
import com.example.pulopo.Utils.UserUtil;
import com.example.pulopo.Utils.UtilsCommon;
import com.example.pulopo.model.ChatMessage;
import com.example.pulopo.model.response.ChatByUserResponse;
import com.google.firebase.storage.FirebaseStorage;
import com.google.firebase.storage.StorageReference;
import com.google.firebase.storage.UploadTask;

import org.json.JSONArray;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.reactivex.rxjava3.android.schedulers.AndroidSchedulers;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;


public class ChatActivity extends AppCompatActivity {
    Toolbar toolbar;
    int iduser;
    String username;
    RecyclerView recyclerView;
    ImageView imgSend, imgLocation, uploadImg,attachFile;
    EditText edtMess;
    ChatAdapter adapter;

    List<ChatMessage> list = new ArrayList<>();
    ApiServer apiServer;
    CompositeDisposable compositeDisposable = new CompositeDisposable();
    String inputGpt = "";
    static String textRs = "";
    private static final int PICK_IMAGE_REQUEST = 1;
    private static final int PICK_FILE_REQUEST = 2;
    StorageReference storageReference;


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_chat);
        iduser = getIntent().getIntExtra("id", 0);//id nguoi nhan
        username = getIntent().getStringExtra("username");
        initView();
        initToolbar();
        apiServer = RetrofitClient.getInstance(UtilsCommon.BASE_URL).create(ApiServer.class);
        initControl();
        storageReference = FirebaseStorage.getInstance().getReference();
        listenMess();

// Khởi chạy đoạn code lần đầu tiên

    }

    private void initControl() {
        imgSend.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                sendMessToServer();
            }
        });
        imgLocation.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                Intent intent = new Intent(ChatActivity.this, MapsActivity.class);
                LocationUtil.setSenderId(UserUtil.getId());
                LocationUtil.setReciverId(iduser);
                startActivity(intent);
            }
        });

    }

    private void sendMessToServer() {
        String str_mess = edtMess.getText().toString().trim();
        inputGpt = str_mess;
        if (TextUtils.isEmpty(str_mess)) {

        } else {
            //call api send mess
            compositeDisposable.add(apiServer.sendMessChat(UserUtil.getId(), iduser, str_mess, 0)
                    .subscribeOn(Schedulers.io())
                    .observeOn(AndroidSchedulers.mainThread())
                    .subscribe(
                            userModel -> {
                                Toast.makeText(getApplicationContext(), "Đã gửi tin nhắn", Toast.LENGTH_LONG).show();
                            },
                            throwable -> {
                                Log.e("SEND", throwable.getMessage());
                                Toast.makeText(getApplicationContext(), throwable.getMessage(), Toast.LENGTH_LONG).show();
                            }
                    ));
            edtMess.setText("");
            if (iduser == 1) {
                gptSendMess();
            }
        }
    }

    private void listenMess() {
        //connect api chuyen ve list<ChatMess>

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        List<ChatMessage> listCheck = new ArrayList<>();
        final int[] number = {0};
        Handler handler = new Handler();
        Runnable runnable = new Runnable() {
            @Override
            public void run() {

                compositeDisposable.add(apiServer.getChatByUser(UserUtil.getId(), iduser, 1)
                        .subscribeOn(Schedulers.io())
                        .observeOn(AndroidSchedulers.mainThread())
                        .subscribe(
                                userModel -> {
                                    ChatByUserResponse chatByUserResponse;
                                    chatByUserResponse = userModel;
                                    for (int i = chatByUserResponse.getData().size() - 1; i >= 0; i--) {
                                        ChatMessage chatMessage = new ChatMessage();
                                        chatMessage.chatId = chatByUserResponse.getData().get(i).getChatId();
                                        chatMessage.sendid = String.valueOf(chatByUserResponse.getData().get(i).getSenderId());
                                        chatMessage.receivedid = String.valueOf(chatByUserResponse.getData().get(i).getReceiverId());
                                        chatMessage.mess = chatByUserResponse.getData().get(i).getMessage();
                                        chatMessage.typeMess = (int) chatByUserResponse.getData().get(i).getMessageType();
                                        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                                            Date date = dateFormat.parse(chatByUserResponse.getData().get(i).getDateSend());
                                            chatMessage.dateObj = date;
                                            chatMessage.datetime = formatDate(date);
                                        }
                                        list.add(chatMessage);
                                    }

                                },
                                throwable -> {
                                    Toast.makeText(getApplicationContext(), throwable.getMessage(), Toast.LENGTH_LONG).show();
                                }
                        ));

                // Đoạn code bạn muốn lặp lại ở đây


                if (number[0] >= 1) {

                    list.subList(0,list.size()).clear();

                    recyclerView.scrollToPosition(list.size());
                }
                adapter = new ChatAdapter(getApplicationContext(), list, String.valueOf(UserUtil.getId()));
                recyclerView.setAdapter(adapter);
                adapter.notifyDataSetChanged();

                number[0]++;

                // Lặp lại đoạn code sau một khoảng thời gian xác định (ví dụ: 1 giây)
                handler.postDelayed(this, 10000);
            }
        };
        handler.post(runnable);


//        if (count==0) {
//            adapter.notifyDataSetChanged();
//
//        }else {
//            adapter.notifyItemRangeChanged(list.size(), list.size());
//            recyclerView.smoothScrollToPosition(list.size() - 1);
//        }


    }

    private String formatDate(Date date) {
        return new SimpleDateFormat("dd MMM, yyyy- hh:mm a", new Locale("en", "US")).format(date);

    }


    private void initView() {

        recyclerView = findViewById(R.id.recycleview_chat);
        imgSend = findViewById(R.id.imagechat);
        imgLocation = findViewById(R.id.imagelocation);
        edtMess = findViewById(R.id.edtinputtext);
        RecyclerView.LayoutManager layoutManager = new LinearLayoutManager(this);
        recyclerView.setLayoutManager(layoutManager);
        recyclerView.setHasFixedSize(true);

        attachFile = findViewById(R.id.attachFile);
        attachFile.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                openFilePicker();
            }
        });
        uploadImg = findViewById(R.id.imagefile);
        uploadImg.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                openImagePicker();
            }
        });
    }
    // attach file
    private void openFilePicker() {
        Intent intent = new Intent(Intent.ACTION_GET_CONTENT);
        intent.setType("*/*");
        startActivityForResult(intent, PICK_FILE_REQUEST);
        Log.e("sendFile","run");
    }
    private void uploadFileToFirebase(Uri fileUri) {
        StorageReference fileRef = storageReference.child("files/" + "pulopo_file_" + formatDate(new Date()));
        String fileName = fileRef.getName();
        UploadTask uploadTask = fileRef.putFile(fileUri);
        uploadTask.addOnSuccessListener(taskSnapshot -> {
            sendFileToServer(fileName);
            Log.e("sendFile","up firebase");
        }).addOnFailureListener(exception -> {
        });
    }

    private void sendFileToServer(String fileName){
        compositeDisposable.add(apiServer.sendMessChat(UserUtil.getId(), iduser, fileName, 3)
                .subscribeOn(Schedulers.io())
                .observeOn(AndroidSchedulers.mainThread())
                .subscribe(
                        userModel -> {
                            Log.e("sendFile","up sv");
                            Toast.makeText(getApplicationContext(), "Đã gửi tập tin", Toast.LENGTH_LONG).show();
                        },
                        throwable -> {

                            Toast.makeText(getApplicationContext(), throwable.getMessage(), Toast.LENGTH_LONG).show();
                        }
                ));
    }
    //Image upload
    private void openImagePicker() {
        Intent intent = new Intent(Intent.ACTION_PICK, MediaStore.Images.Media.EXTERNAL_CONTENT_URI);
        startActivityForResult(intent, PICK_IMAGE_REQUEST);
    }

    @Override
    protected void onActivityResult(int requestCode, int resultCode, Intent data) {
        super.onActivityResult(requestCode, resultCode, data);

        if (requestCode == PICK_IMAGE_REQUEST && resultCode == RESULT_OK && data != null && data.getData() != null) {
            // The user has successfully picked an image
            Uri selectedImageUri = data.getData();
            uploadImageToFirebase(selectedImageUri);
        }
        if (requestCode == PICK_FILE_REQUEST && resultCode == RESULT_OK && data != null && data.getData() != null) {
            // The user has successfully picked an file

            Uri selectedFileUri = data.getData();
            uploadFileToFirebase(selectedFileUri);
            Log.e("sendFile","rs");
        }

    }



    private void uploadImageToFirebase(Uri imageUri) {
        // Create a reference to 'images/<FILENAME>'
        StorageReference imageRef = storageReference.child("images/" + formatDate(new Date()));

        String fileName = imageRef.getName();

        // Upload the file to Firebase Storage
        UploadTask uploadTask = imageRef.putFile(imageUri);

        // Register observers to listen for when the upload is done or if it fails
        uploadTask.addOnSuccessListener(taskSnapshot -> {
            // Image uploaded successfully, handle the success
            // You can get the download URL of the uploaded image from taskSnapshot.getDownloadUrl()
            sendImageToServer(fileName);

        }).addOnFailureListener(exception -> {
            // Handle unsuccessful uploads
            // ...
        });
    }

    private void sendImageToServer(String path) {
        compositeDisposable.add(apiServer.sendMessChat(UserUtil.getId(), iduser, path, 2)
                .subscribeOn(Schedulers.io())
                .observeOn(AndroidSchedulers.mainThread())
                .subscribe(
                        userModel -> {
                            Toast.makeText(getApplicationContext(), "Đã gửi ảnh", Toast.LENGTH_LONG).show();
                        },
                        throwable -> {

                            Toast.makeText(getApplicationContext(), throwable.getMessage(), Toast.LENGTH_LONG).show();
                        }
                ));

    }

    private void initToolbar() {
        toolbar = findViewById(R.id.toolbarChat);
        toolbar.setTitle(username);
    }

    public String getContentGPt(String textPost) {

        String text = "";
        String apiKey = "-wh8OX5OyjOvwf5Rv399ZT3BlbkFJEwpNBQYllStWP60hMv8Q";
        String endpoint = "https://api.openai.com/v1/engines/gpt-3.5-turbo-instruct/completions";

        OkHttpClient client = new OkHttpClient();
        String rs = textPost.replace('\"', ' ').replace('{', ' ').replace('}', ' ').replace(',', ' ').replace('[', ' ')
                .replace(']', ' ').replace(':', ' ').replace('/', ' ').replace('\\', ' ').replace('\'', ' ');

        String input = rs.replace("\n", " ");

        byte[] inputByte = input.getBytes();

        String requestInput = new String(inputByte, StandardCharsets.UTF_8);
        MediaType mediaType = MediaType.parse("application/json");
        RequestBody body = RequestBody.create(mediaType, "{\"prompt\": \"" + input + "\", \"max_tokens\": 200}");
        Request request = new Request.Builder().url(endpoint).post(body).addHeader("Authorization", "Bearer " + apiKey)
                .build();

        CompletableFuture<String> future = new CompletableFuture<>();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(() -> {
            try {
                Response response = client.newCall(request).execute();
                byte[] responseBodyBytes = response.body().bytes();
                String responseBody = new String(responseBodyBytes, StandardCharsets.UTF_8);
                JSONObject json = new JSONObject(responseBody);
                System.out.println(json);
                JSONArray choices = json.getJSONArray("choices");
                JSONObject choice = choices.getJSONObject(0);
                textRs = choice.getString("text");
                future.complete(textRs);
            } catch (Exception e) {
                e.printStackTrace();
                future.completeExceptionally(e);
            }
        });

        try {
            return future.get(); // This will block until the future is complete
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            return null;
        }
    }

    public void gptSendMess() {
        Log.e("GPT", inputGpt);
        CompletableFuture.runAsync(() -> {
            String gptResponse = getContentGPt(inputGpt);
            // Use the GPT response as needed
            // Example: Send the GPT response to the server
            compositeDisposable.add(apiServer.sendMessChat(1, UserUtil.getId(), gptResponse, 0)
                    .subscribeOn(Schedulers.io())
                    .observeOn(AndroidSchedulers.mainThread())
                    .subscribe(
                            userModel -> {
                                Toast.makeText(getApplicationContext(), "Đã gửi tin nhắn", Toast.LENGTH_LONG).show();
                            },
                            throwable -> {
                                Toast.makeText(getApplicationContext(), throwable.getMessage(), Toast.LENGTH_LONG).show();
                            }
                    ));
        });
    }
}