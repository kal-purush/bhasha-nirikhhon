package help.desk.mobile.app.fragment;

import android.content.Context;
import android.content.Intent;
import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.EditText;

import com.example.helpdeskmobile.R;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.fragment.app.Fragment;
import butterknife.BindView;
import butterknife.ButterKnife;
import butterknife.OnClick;
import help.desk.mobile.app.activity.HomeActivity;
import help.desk.mobile.app.model.APIError;
import help.desk.mobile.app.model.Status;
import help.desk.mobile.app.model.TicketDetails;
import help.desk.mobile.app.model.request.EvaluateTicketRequest;
import help.desk.mobile.app.service.BaseService;
import help.desk.mobile.app.service.TicketService;
import help.desk.mobile.app.utils.ErrorUtils;
import help.desk.mobile.app.utils.ToastUtils;
import lombok.NoArgsConstructor;
import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;

import static help.desk.mobile.app.constants.ResultMessages.TICKET_APPROVED_WITH_SUCCESS;
import static help.desk.mobile.app.constants.ResultMessages.TICKET_REFUSED_WITH_SUCCESS;
import static help.desk.mobile.app.model.Status.APPROVED;
import static help.desk.mobile.app.model.Status.DENIED;

@NoArgsConstructor
public class TicketAdminActionsFragment extends Fragment {

    private TicketDetails ticket;

    private TicketService ticketService;

    @BindView(R.id.ticket_admin_message)
    protected EditText ticketMessageEditText;

    @OnClick(R.id.approve_ticket_button)
    protected void approveTicket() {
        evaluateTicket(APPROVED, TICKET_APPROVED_WITH_SUCCESS);
    }

    @OnClick(R.id.approve_ticket_button)
    protected void refuseTicket() {
        evaluateTicket(DENIED, TICKET_REFUSED_WITH_SUCCESS);
    }

    private void evaluateTicket(final Status status, final String message) {
        this.ticketService.evaluateTicket(this.ticket.getId(), buildEvaluateTicketRequest(APPROVED)).enqueue(new Callback<Void>() {
            @Override
            public void onResponse(Call<Void> call, Response<Void> response) {
                if (!response.isSuccessful()) {
                    APIError error = ErrorUtils.parseError(response);
                    ToastUtils.displayMessage(TicketAdminActionsFragment.this.getContext(), error.getMessage());
                    return;
                }
                handleSuccessfulResponse(message);
            }

            @Override
            public void onFailure(Call<Void> call, Throwable t) {
                ToastUtils.displayErrorWhileCallingServer(TicketAdminActionsFragment.this.getContext());
            }
        });
    }

    private void handleSuccessfulResponse(final String message) {
        final Context context = TicketAdminActionsFragment.this.getContext();
        ToastUtils.displayMessage(context, message);
        final Intent intent = new Intent(context, HomeActivity.class);
        context.startActivity(intent);
    }


    private EvaluateTicketRequest buildEvaluateTicketRequest(Status status) {
        return EvaluateTicketRequest.builder()
                .newStatus(status)
                .curatorshipMessage(ticketMessageEditText.getText().toString())
                .build();
    }

    @Override
    public View onCreateView(@NonNull LayoutInflater inflater, @Nullable ViewGroup container, @Nullable Bundle savedInstanceState) {
        this.ticket = (TicketDetails) getArguments().getSerializable("ticket");
        this.ticketService = new BaseService(getContext()).getTicketService();

        final View view = inflater.inflate(R.layout.fragment_ticket_admin_actions, container, false);
        ButterKnife.bind(this, view);
        return view;
    }

}