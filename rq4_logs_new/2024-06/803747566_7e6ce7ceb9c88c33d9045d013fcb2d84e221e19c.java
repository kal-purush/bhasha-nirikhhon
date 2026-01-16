package fcu.midterm.bookstore;

import android.content.Context;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.SimpleAdapter;
import android.widget.TextView;

import androidx.annotation.NonNull;

import com.google.firebase.auth.FirebaseAuth;
import com.google.firebase.database.DataSnapshot;
import com.google.firebase.database.DatabaseError;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;
import com.google.firebase.database.Query;
import com.google.firebase.database.ValueEventListener;

import java.util.List;
import java.util.Map;

public class HistoryAdapter extends SimpleAdapter {
    private Context context;
    private List<Map<String, String>> data;
    private int resource;
    private String[] from;
    private int[] to;
    private FirebaseAuth mAuth;

    public HistoryAdapter(Context context, List<Map<String, String>> data, int resource, String[] from, int[] to) {
        super(context, data, resource, from, to);
        this.context = context;
        this.data = data;
        this.resource = resource;
        this.from = from;
        this.to = to;
        this.mAuth = FirebaseAuth.getInstance();
    }

    @Override
    public View getView(int position, View convertView, ViewGroup parent) {
        View view = super.getView(position, convertView, parent);

        Button borrowButton = view.findViewById(R.id.btn_return_borrowAgain);
        Button returnButton = view.findViewById(R.id.btn_returnBook);
        TextView bookState = view.findViewById(R.id.tv_return_state);
        String email = mAuth.getCurrentUser().getEmail();

        // 获取当前条目数据
        Map<String, String> currentItem = data.get(position);
        String bookName = currentItem.get("bookName");

        FirebaseDatabase database = FirebaseDatabase.getInstance();
        DatabaseReference ref = database.getReference("borrow_history");
        Query query = ref.orderByChild("bookName").equalTo(bookName);

        // 查询数据库，获取借阅人姓名
        query.addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(@NonNull DataSnapshot snapshot) {
                for (DataSnapshot ds : snapshot.getChildren()) {
                    String personName = ds.child("personName").getValue(String.class);

                    // 设置按钮的可用性
                    if ("借閱中".equals(bookState.getText().toString())) {
                        borrowButton.setEnabled(false);
                        if (email.equals(personName)) {
                            returnButton.setEnabled(true);
                        } else {
                            returnButton.setEnabled(false);
                        }
                    } else {
                        borrowButton.setEnabled(true);
                        returnButton.setEnabled(false);
                    }
                }
            }

            @Override
            public void onCancelled(@NonNull DatabaseError error) {
                // 处理数据库查询错误
                borrowButton.setEnabled(false);
                returnButton.setEnabled(false);
            }
        });

        return view;
    }
}