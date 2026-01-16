package kr.ac.kpu.block.smared;

import com.google.firebase.database.DataSnapshot;
import com.google.firebase.database.DatabaseError;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;
import com.google.firebase.database.ValueEventListener;

import java.util.Map;
import java.util.function.Consumer;

public class DAO {
    private FormattedLogger logger = new FormattedLogger();

    private Consumer<DAO> successCallback;
    private Consumer<DAO> failCallback;

    public void setSuccessCallback(Consumer<DAO> successCallback) {
        this.successCallback = successCallback;
    }

    public void setFailureCallback(Consumer<DAO> failCallback) {
        this.failCallback = failCallback;
    }

    // 경로가 입력되지 않은 경우 기본 경로 DB에 데이터를 저장한다.
    public void create(DTO dto, Class clazz) {
        String databasePath = clazz.getSimpleName() + "/" + dto.getUid();
        create(dto, clazz, databasePath);
    }
    // DB에 데이터를 저장한다.
    public void create(DTO dto, Class clazz, String databasePath) {
        DatabaseReference databaseReference = FirebaseDatabase.getInstance().getReference(databasePath);

        Map<String, Object> map = dto.toMap();
        databaseReference.setValue(map).addOnCompleteListener(task -> {
            if (task.isSuccessful()) {
                logger.writeLog("Success");
                if (successCallback != null) {
                    successCallback.accept(this);
                }
            } else {
                logger.writeLog("Fail");
                if (failCallback != null) {
                    failCallback.accept(this);
                }
            }
        });
    }

    public void read(DTO dto, Class clazz, String databasePath) {
        DatabaseReference databaseReference = FirebaseDatabase.getInstance().getReference(databasePath);

        // DB 읽어오기
        databaseReference.addValueEventListener(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot userSnapshot) {
                logger.writeLog("Success");
                //[TODO] 읽은 데이터를 전달할 방법을 찾아야 한다.
                //dto = new Ledger();
            }

            @Override
            public void onCancelled(DatabaseError error) {
                logger.writeLog("Failed to read value : " + error.toException().getMessage());
            }
        });
    }

    public void update() {

//        // 토큰으로 로그인 상태를 관리한다.
//        DatabaseReference myRef = FirebaseDatabase.getInstance().getReference("users");
//        Map<String, Object> token = new HashMap<>();
//        token.put("fcmToken", FirebaseInstanceId.getInstance().getToken());
//        myRef.child(user.getUid()).updateChildren(token);
    }

    public void delete() {

    }


}