package nrega.worker.Adapters;

import android.support.v7.widget.RecyclerView;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;

import java.util.List;

import nrega.worker.Model.FamilyMember;
import nrega.worker.R;

/**
 * Created by HP on 22-01-2018.
 */

public class FamilyMemberAdapter extends RecyclerView.Adapter<FamilyMemberAdapter.MyViewHolder>{

    List<FamilyMember> members;

    public class MyViewHolder extends RecyclerView.ViewHolder{
        public TextView member_name,member_dob,member_bankname;

        public MyViewHolder(View itemView) {
            super(itemView);
            member_name = (TextView) itemView.findViewById(R.id.member_name);
            member_dob = (TextView) itemView.findViewById(R.id.member_dob );
            member_bankname = (TextView) itemView.findViewById(R.id.member_bankname);
        }
    }

    public FamilyMemberAdapter(List<FamilyMember> members) {
        this.members = members;
    }

    @Override
    public FamilyMemberAdapter.MyViewHolder onCreateViewHolder(ViewGroup viewGroup, int i) {
        View itemView = LayoutInflater.from(viewGroup.getContext())
                .inflate(R.layout.family_member_details,viewGroup,false);

        return new FamilyMemberAdapter.MyViewHolder(itemView);
    }

    @Override
    public void onBindViewHolder(FamilyMemberAdapter.MyViewHolder myViewHolder, int position) {
        FamilyMember member = members.get(position);

        myViewHolder.member_name.setText(member.getName());
        myViewHolder.member_dob.setText(member.getDOB());
        myViewHolder.member_bankname.setText(member.getBankName());
    }

    @Override
    public int getItemCount() {
        return members.size();
    }
}