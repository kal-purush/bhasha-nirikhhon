package org.naturenet;

import android.os.Parcel;
import android.support.test.runner.AndroidJUnit4;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.naturenet.data.model.Comment;

import static org.junit.Assert.assertEquals;

/**
 * Created by rigot on 2/18/2017.
 */

@RunWith(AndroidJUnit4.class)
public class CommentParcelizationTest {

    static String ID = "someID";
    static String COMMENT = "someComment";
    static String PERSON = "somePerson";
    static String PARENT = "someParent";
    static String CONTEXT = "someContext";

    @Test
    public void testComment() {

        Comment testComment = Comment.createNew(ID, COMMENT, PERSON, PARENT, CONTEXT);

        Parcel parcel = Parcel.obtain();

        testComment.writeToParcel(parcel, 0);

        parcel.setDataPosition(0);

        Comment parceledComment = Comment.CREATOR.createFromParcel(parcel);

        assertEquals(null, parceledComment.id, testComment.id);
        assertEquals(null, parceledComment.comment, testComment.comment);
        assertEquals(null, parceledComment.commenter, testComment.commenter);
        assertEquals(null, parceledComment.parent, testComment.parent);
        assertEquals(null, parceledComment.context, testComment.context);


    }
}