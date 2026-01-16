package wackycodes.gowacky.W_Fragment;

import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.TextView;
import android.widget.ListAdapter;
import java.util.ArrayList;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;
import wackycodes.gowacky.R;
import wackycodes.gowacky.Go_Activity11;
import wackycodes.gowacky.W_Fragment.W_FragmentOneModelClass;
import wackycodes.gowacky.W_Fragment.W_FragmentOneReqOffRide;


public class W_FragmentOneAdaptorClass extends RecyclerView.Adapter<W_FragmentOneAdaptorClass.RequestRideViewHolder> {

    private ArrayList<W_FragmentOneModelClass> wFragmentOneModelClassArrayList;

    public static class RequestRideViewHolder extends RecyclerView.ViewHolder{
        // variable declaration...
        public ImageView wVehicleImage;
        public TextView wVehicleType;
        public TextView wAvailableSeats,wVehicleReachTime;

        public RequestRideViewHolder(@NonNull View itemView) {
            super(itemView);
            wVehicleType = itemView.findViewById(R.id.wVehicleType);
            wAvailableSeats = itemView.findViewById(R.id.wAvailableSeats);
            wVehicleReachTime = itemView.findViewById(R.id.wVehicleReachTime);
            wVehicleImage = itemView.findViewById(R.id.wVehicleImage);
        }
    }

//    Constructors....----------------------------------
    public W_FragmentOneAdaptorClass(){

    }
    public W_FragmentOneAdaptorClass(ArrayList<W_FragmentOneModelClass> wFragmentOneModelClassArrayList){
        this.wFragmentOneModelClassArrayList = wFragmentOneModelClassArrayList;
    }
//    Constructors....----------------------------------


    @NonNull
    @Override
    public RequestRideViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        View view = LayoutInflater.from(parent.getContext()).inflate(R.layout.w_fragment_one_requst_ride_item,parent,false);
        RequestRideViewHolder requestRideViewHolder = new RequestRideViewHolder( view );
        return requestRideViewHolder;
    }

    @Override
    public void onBindViewHolder(@NonNull RequestRideViewHolder holder, int position) {
        W_FragmentOneModelClass w_fragmentOneModelClass = wFragmentOneModelClassArrayList.get(position);
        holder.wVehicleType.setText(Integer.toString(w_fragmentOneModelClass.getwVehicleType()));
        holder.wAvailableSeats.setText(Integer.toString(w_fragmentOneModelClass.getwAvailableSeats()));
        holder.wVehicleReachTime.setText(Integer.toString(w_fragmentOneModelClass.getwVehicleReachTime()));
        holder.wVehicleImage.setImageResource(w_fragmentOneModelClass.getwVehicleImage());
    }

    @Override
    public int getItemCount() {
        return wFragmentOneModelClassArrayList.size();
    }


}