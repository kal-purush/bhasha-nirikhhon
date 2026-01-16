import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;

/**
 * Created by arjun on 8/10/15.
 */
public class ArrayTools {
    public static ArrayList<String> jsonArrToArrList( JSONArray jsonArr ) throws JSONException {
        ArrayList<String> out = new ArrayList<String>();
        if( jsonArr != null ) {
            int len = jsonArr.length();
            for( int i = 0; i < len; i++ )
                out.add( jsonArr.get(i).toString() );
        }
        return out;
    }
}