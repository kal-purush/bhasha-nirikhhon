package finalproject_plants;

import android.app.Activity;
import android.os.Bundle;

public class plants extends Activity {
    public int type;



    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        type = getIntent().getFlags();
        switch (type) {
            case 1:
                setContentView(R.layout.photinia);
                break;
            case 2:
                setContentView(R.layout.planch);
                break;
            case 3:
                setContentView(R.layout.jacq);
                break;
            case 4:
                setContentView(R.layout.hance);
                break;
            case 5:
                setContentView(R.layout.linn);
                break;
            default:
                break;
        }
    }
}
