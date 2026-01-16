package help.desk.mobile.app.activity;

import android.os.Bundle;
import android.widget.EditText;

import com.example.helpdeskmobile.R;

import androidx.appcompat.app.AppCompatActivity;
import androidx.fragment.app.Fragment;
import androidx.fragment.app.FragmentManager;
import androidx.fragment.app.FragmentTransaction;
import butterknife.BindView;
import butterknife.ButterKnife;
import help.desk.mobile.app.fragment.TicketAdminActionsFragment;
import help.desk.mobile.app.fragment.TicketUserActionsFragment;
import help.desk.mobile.app.model.ProfileType;
import help.desk.mobile.app.model.TicketDetails;

import static help.desk.mobile.app.model.ProfileType.ADMIN;

public class TicketDetailsActivity extends AppCompatActivity {

    private TicketDetails ticket;

    private FragmentManager fragmentManager = getSupportFragmentManager();

    @BindView(R.id.ticket_details_title)
    protected EditText titleEditText;

    @BindView(R.id.ticket_details_description)
    protected EditText descriptionEditText;

    @BindView(R.id.ticket_details_area)
    protected EditText areaEditText;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        this.ticket = (TicketDetails) getIntent().getExtras().getSerializable("ticket");
        setContentView(R.layout.activity_ticket_details);
        renderActions();
        ButterKnife.bind(this);
        titleEditText.setText(ticket.getTitle());
        descriptionEditText.setText(ticket.getDescription());
        areaEditText.setText(ticket.getArea().getName());
    }

    private void renderActions() {
        final ProfileType loggedUserRole = getLoggedUserRole();
        if (ADMIN.equals(loggedUserRole)) {
            renderFragment(new TicketAdminActionsFragment());
            return;
        }
        renderFragment(new TicketUserActionsFragment());
    }

    private ProfileType getLoggedUserRole() {
        final String role = getSharedPreferences("user_details", MODE_PRIVATE).getString("userRole", "DEFAULT");
        return ProfileType.valueOf(role);
    }

    private void renderFragment(final Fragment fragmentToRender) {
        Bundle bundle = new Bundle();
        bundle.putSerializable("ticket", ticket);

        fragmentToRender.setArguments(bundle);

        FragmentTransaction fragmentTransaction = fragmentManager.beginTransaction();
        fragmentTransaction.replace(R.id.ticket_actions_layout, fragmentToRender);
        fragmentTransaction.commit();
    }
}