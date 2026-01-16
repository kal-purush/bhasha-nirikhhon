package vn.edu.usth.outlook.adapter;

import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;
import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;
import java.util.List;
import vn.edu.usth.outlook.Email_Sent;
import vn.edu.usth.outlook.Email_receiver;
import vn.edu.usth.outlook.R;
import vn.edu.usth.outlook.listener.SelectListener;

public class ArchivedAdapter extends RecyclerView.Adapter<RecyclerView.ViewHolder> {
    private static final int TYPE_RECEIVER = 1;
    private static final int TYPE_SENT = 2;

    private Context context;
    private List<Object> archivedEmails;
    private SelectListener selectListener;

    public ArchivedAdapter(Context context, List<Object> archivedEmails, SelectListener selectListener) {
        this.context = context;
        this.archivedEmails = archivedEmails;
        this.selectListener = selectListener;
    }

    @Override
    public int getItemViewType(int position) {
        if (archivedEmails.get(position) instanceof Email_receiver) {
            return TYPE_RECEIVER;
        } else {
            return TYPE_SENT;
        }
    }

    @NonNull
    @Override
    public RecyclerView.ViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        if (viewType == TYPE_RECEIVER) {
            View view = LayoutInflater.from(context).inflate(R.layout.item_inbox, parent, false);
            return new DeletedInboxViewHolder(view, selectListener);
        } else {
            View view = LayoutInflater.from(context).inflate(R.layout.item_sent, parent, false);
            return new DeletedSentViewHolder(view, selectListener);
        }
    }

    @Override
    public void onBindViewHolder(@NonNull RecyclerView.ViewHolder holder, int position) {
        holder.itemView.setOnClickListener(v -> selectListener.onItemClicked(position)); // Xử lý sự kiện nhấp
        if (holder instanceof DeletedInboxViewHolder) {
            ((DeletedInboxViewHolder) holder).bind((Email_receiver) archivedEmails.get(position));
        } else {
            ((DeletedSentViewHolder) holder).bind((Email_Sent) archivedEmails.get(position));
        }
    }

    @Override
    public int getItemCount() {
        return archivedEmails.size();
    }

    static class DeletedInboxViewHolder extends RecyclerView.ViewHolder {
        TextView sender, subject, content;

        DeletedInboxViewHolder(View itemView, SelectListener listener) {
            super(itemView);
            sender = itemView.findViewById(R.id.nameReceive);
            subject = itemView.findViewById(R.id.head_emailReceive);
            content = itemView.findViewById(R.id.contentReceive);

            // Truyền listener vào sự kiện click
            itemView.setOnClickListener(v -> listener.onItemClicked(getAdapterPosition()));
        }

        void bind(Email_receiver email) {
            sender.setText(email.getSender());
            subject.setText(email.getSubject());
            content.setText(email.getContent());
        }
    }

    static class DeletedSentViewHolder extends RecyclerView.ViewHolder {
        TextView receiver, subject, content;

        DeletedSentViewHolder(View itemView, SelectListener listener) {
            super(itemView);
            receiver = itemView.findViewById(R.id.nameSent);
            subject = itemView.findViewById(R.id.head_emailSent);
            content = itemView.findViewById(R.id.contentSent);

            // Truyền listener vào sự kiện click
            itemView.setOnClickListener(v -> listener.onItemClicked(getAdapterPosition()));
        }

        void bind(Email_Sent email) {
            receiver.setText(email.getReceiver());
            subject.setText(email.getSubject());
            content.setText(email.getContent());
        }
    }
}