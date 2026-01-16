package hungnp12.demo.daubep;

import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import org.w3c.dom.Text;

import java.util.List;
import java.util.zip.Inflater;

import de.hdodenhof.circleimageview.CircleImageView;
import hungnp12.demo.daubep.model.Comment;

public class CommentAdapter extends RecyclerView.Adapter<CommentAdapter.CommentViewHolder> {
    List<Comment> cmtList;
    public CommentAdapter(List<Comment> cmtList) {
        this.cmtList = cmtList;
    }

    @NonNull
    @Override
    public CommentViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        View view = LayoutInflater.from(parent.getContext()).inflate(R.layout.comment_item,parent,false);
        CommentViewHolder viewHolder = new CommentViewHolder(view);
        return viewHolder;
    }

    @Override
    public void onBindViewHolder(@NonNull CommentViewHolder holder, int position) {
        Comment cmt = cmtList.get(position);
        holder.cmtAvt.setImageResource(cmt.getImage());
        holder.cmtName.setText(cmt.getUserName());
        holder.cmtContent.setText(cmt.getCmtContent());
    }

    @Override
    public int getItemCount() {
        return cmtList.size();
    }

    public static class CommentViewHolder extends RecyclerView.ViewHolder{
        CircleImageView cmtAvt;
        TextView cmtName;
        TextView cmtContent;
       public CommentViewHolder(@NonNull View itemView) {
           super(itemView);
           cmtAvt = itemView.findViewById(R.id.cmtAvt);
           cmtName = itemView.findViewById(R.id.cmtName);
           cmtContent = itemView.findViewById(R.id.cmtContent);
       }
   }
}