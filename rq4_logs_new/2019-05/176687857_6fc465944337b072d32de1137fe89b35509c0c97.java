package it.polito.mad.customer;

import android.content.Context;
import android.net.Uri;
import android.os.Bundle;
import android.support.annotation.Nullable;
import android.support.v4.app.Fragment;
import android.support.v7.widget.LinearLayoutManager;
import android.support.v7.widget.RecyclerView;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;

import java.util.List;


/**
 * A simple {@link Fragment} subclass.
 * Activities that contain this fragment must implement the
 * {@link OrdersCompletedFragment.OnFragmentInteractionListenerComplete} interface
 * to handle interaction events.
 * Use the {@link OrdersCompletedFragment#newInstance} factory method to
 * create an instance of this fragment.
 */
public class OrdersCompletedFragment extends Fragment {
    private static final String ARG_PARAM1 = "param1";
    private static final String ARG_PARAM2 = "param2";

    private RecyclerView recyclerView;
    private RVAOrders adapter;
    private View view;

    // TODO: Rename and change types of parameters
    private String mParam1;
    private String mParam2;
    private List<OrdersInfo> ordersInfos;
    private Context context;


    private OrdersCompletedFragment.OnFragmentInteractionListenerComplete mListener;

    public OrdersCompletedFragment() {
        // Required empty public constructor
    }

    public void refreshLayout() {
        initRecyclerView();
    }

    public List<OrdersInfo> getOrdersInfos() {
        return ordersInfos;
    }

    public OrdersCompletedFragment setOrderInfos(List<OrdersInfo> ordersInfos) {
        this.ordersInfos = ordersInfos;
        //Log.d("TAG", "setSuggestedFoodInfos: DailyFoodFragment");
        return this;
    }

    @Nullable
    @Override
    public Context getContext() {
        return context;
    }

    public OrdersCompletedFragment setContext(Context context) {
        this.context = context;
        return this;
    }

    /**
     * Use this factory method to create a new instance of
     * this fragment using the provided parameters.
     *
     * @param param1 Parameter 1.
     * @param param2 Parameter 2.
     * @return A new instance of fragment DailyFoodFragment.
     */
    // TODO: Rename and change types and number of parameters
    public static OrdersPendingFragment newInstance(String param1, String param2) {
        OrdersPendingFragment fragment = new OrdersPendingFragment();
        Bundle args = new Bundle();
        args.putString(ARG_PARAM1, param1);
        args.putString(ARG_PARAM2, param2);
        fragment.setArguments(args);
        return fragment;
    }

    @Override
    public void onCreate(Bundle savedInstanceState) {
        //Log.d("TAG", "onCreate: DailyFoodFragment");
        super.onCreate(savedInstanceState);
        if (getArguments() != null) {
            mParam1 = getArguments().getString(ARG_PARAM1);
            mParam2 = getArguments().getString(ARG_PARAM2);
        }


    }

    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container,
                             Bundle savedInstanceState) {
        //Log.d("TAG", "onCreateView: DailyFoodFragment");
        //scaricare i dati nell'attivita principale e passare i dati come parametro di costruzione

        view = inflater.inflate(R.layout.fragment_orders_completed, container, false);
        recyclerView = (RecyclerView) view.findViewById(R.id.rv_completed);

        initRecyclerView();

        // Inflate the layout for this fragment
        return view;
    }

    private void initRecyclerView() {

        recyclerView.setHasFixedSize(false);

        LinearLayoutManager llm = new LinearLayoutManager(context);
        recyclerView.setLayoutManager(llm);

        adapter = new RVAOrders(context, ordersInfos);
        recyclerView.setAdapter(adapter);
    }

    // TODO: Rename method, update argument and hook method into UI event
    public void onButtonPressed(Uri uri) {
        if (mListener != null) {
            mListener.onFragmentInteractionComplete(uri);
        }
    }

    @Override
    public void onAttach(Context context) {
        super.onAttach(context);
        //Log.d("TAG", "onAttach: DailyFoodFragment");
        if (context instanceof OrdersCompletedFragment.OnFragmentInteractionListenerComplete) {
            mListener = (OrdersCompletedFragment.OnFragmentInteractionListenerComplete) context;
        } else {
            throw new RuntimeException(context.toString()
                    + " must implement OnFragmentInteractionListener");
        }
    }

    @Override
    public void onDetach() {
        super.onDetach();
        mListener = null;
    }

    /**
     * This interface must be implemented by activities that contain this
     * fragment to allow an interaction in this fragment to be communicated
     * to the activity and potentially other fragments contained in that
     * activity.
     * <p>
     * See the Android Training lesson <a href=
     * "http://developer.android.com/training/basics/fragments/communicating.html"
     * >Communicating with Other Fragments</a> for more information.
     */
    public interface OnFragmentInteractionListenerComplete {
        // TODO: Update argument type and name
        void onFragmentInteractionComplete(Uri uri);
    }
}