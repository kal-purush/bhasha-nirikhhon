package fao;
import com.google.firebase.database.ChildEventListener;
import com.google.firebase.database.DataSnapshot;
import com.google.firebase.database.DatabaseError;
import com.google.firebase.database.DatabaseException;
import com.google.firebase.database.DatabaseReference;
import model.LearningTrial;
import java.util.ArrayList;

/**
 * Created by Asif on 3/17/2018.
 */

public class ManageLearningTrail {
    DatabaseReference db;
    Boolean saved=null;
    ArrayList<LearningTrial> learningTrials=new ArrayList<>();


    public ManageLearningTrail(DatabaseReference db) {
        this.db = db;
    }

    //WRITE IF NOT NULL
    public Boolean save(LearningTrial learningTrial)
    {
        if(learningTrial==null)
        {
            saved=false;
        }else
        {
            try
            {
                db.child("LearningTrial").push().setValue(learningTrial);
                saved=true;

            }catch (DatabaseException e)
            {
                e.printStackTrace();
                saved=false;
            }
        }

        return saved;
    }

    //IMPLEMENT FETCH DATA AND FILL ARRAYLIST
    private void fetchData(DataSnapshot dataSnapshot)
    {
        learningTrials.clear();

        for (DataSnapshot ds : dataSnapshot.getChildren())
        {
            LearningTrial learningTrial=ds.getValue(LearningTrial.class);
            learningTrials.add(learningTrial);
        }
    }

    //READ THEN RETURN ARRAYLIST
    public ArrayList<LearningTrial> retrieve() {
        db.addChildEventListener(new ChildEventListener() {
            @Override
            public void onChildAdded(DataSnapshot dataSnapshot, String s) {
                fetchData(dataSnapshot);
            }

            @Override
            public void onChildChanged(DataSnapshot dataSnapshot, String s) {
                fetchData(dataSnapshot);

            }

            @Override
            public void onChildRemoved(DataSnapshot dataSnapshot) {

            }

            @Override
            public void onChildMoved(DataSnapshot dataSnapshot, String s) {

            }

            @Override
            public void onCancelled(DatabaseError databaseError) {

            }
        });
        return learningTrials;
    }

}
