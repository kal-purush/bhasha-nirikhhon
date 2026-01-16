package com.cowboydadas.book_logger;

import android.os.Bundle;
import android.support.design.widget.Snackbar;
import android.support.v7.app.AppCompatActivity;
import android.support.v7.widget.Toolbar;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.ImageView;
import android.widget.TextView;
import android.widget.Toast;

import com.cowboydadas.book_logger.db.BookLoggerDbHelper;
import com.cowboydadas.book_logger.exception.BookHistoryException;
import com.cowboydadas.book_logger.model.Book;
import com.cowboydadas.book_logger.util.BookUtility;
import com.squareup.picasso.Picasso;

public class BookProcessActivity extends AppCompatActivity {

    public static final String BOOK_PROCESS = "BookProcessActivity_";
    private EditText etextCurrentPage;
    private Book activeBook;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_book_process);
        Toolbar toolbar = (Toolbar) findViewById(R.id.toolbar);
        setSupportActionBar(toolbar);
        getSupportActionBar().setDisplayHomeAsUpEnabled(true);

        etextCurrentPage = findViewById(R.id.editTextCurrentPage);
        ImageView imgBookCover = findViewById(R.id.imgBookCover);
        TextView txtBookTitle = findViewById(R.id.txtBookTitle);
        TextView txtBookTotalPage = findViewById(R.id.txtTotalPage);
        Bundle bundle = getIntent().getExtras();
        if (bundle != null && bundle.containsKey(BOOK_PROCESS)) {
            String bookJson = bundle.getString(BOOK_PROCESS);
            activeBook = (Book) BookUtility.convertJSON2Object(bookJson, Book.class);
            txtBookTitle.setText(String.valueOf(activeBook.getTitle()));
            txtBookTotalPage.setText(String.valueOf(activeBook.getTotalPage()));
            if (BookUtility.isNullOrEmpty(activeBook.getCover())) {
//                Picasso.with(this).cancelRequest(imgBookCover);
                Picasso.with(this)
                        .load("file:" + activeBook.getCover())
                        .resizeDimen(R.dimen.img_width, R.dimen.img_height)
                        .centerCrop()
                        .into(imgBookCover);
            }
        }

        Button btnAddProcess = findViewById(R.id.btnAddProcess);
        btnAddProcess.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                addProcess();
            }
        });
    }

    private void addProcess() {
        int curPage = etextCurrentPage.getText().toString().trim().length()>0 ? Integer.valueOf(etextCurrentPage.getText().toString()) : 0;
        if (activeBook != null && curPage > 0){
            activeBook.setCurrentPage(curPage);
            try {
                if (activeBook.getCurrentPage() > activeBook.getTotalPage()){
                    Toast.makeText(getApplicationContext(), "Bulunduğunuz sayfa toplam sayfadan büyük olamaz", Toast.LENGTH_LONG).show();
                }else {
                    long id = BookLoggerDbHelper.getInstance(getApplicationContext()).insertBookProcess(activeBook);
                    Snackbar.make(getCurrentFocus(), "Kayıt başarıyla eklendi", Snackbar.LENGTH_SHORT);
                }
            } catch (BookHistoryException e) {
                e.printStackTrace();
                Toast.makeText(getApplicationContext(), e.getMessage(), Toast.LENGTH_LONG).show();
            }
        }
    }

}