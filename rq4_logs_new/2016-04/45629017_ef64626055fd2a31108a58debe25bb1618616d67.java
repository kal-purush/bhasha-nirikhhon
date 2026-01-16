package com.simbiosyscorp.thetravelingsalesman.view;

import android.content.Context;
import android.content.Intent;
import android.os.Bundle;
import android.support.design.widget.TabLayout;
import android.support.v4.app.Fragment;
import android.support.v4.app.FragmentManager;
import android.support.v4.app.FragmentPagerAdapter;
import android.support.v4.view.ViewPager;
import android.util.Log;
import android.view.ContextMenu;
import android.view.LayoutInflater;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.ImageButton;
import android.widget.LinearLayout;
import android.widget.TextView;
import android.widget.Toast;

import com.simbiosyscorp.thetravelingsalesman.R;
import com.simbiosyscorp.thetravelingsalesman.database.TripCursorWrapper;
import com.simbiosyscorp.thetravelingsalesman.dialog.DatePickerFragment;
import com.simbiosyscorp.thetravelingsalesman.models.Client;
import com.simbiosyscorp.thetravelingsalesman.models.ClientManager;
import com.simbiosyscorp.thetravelingsalesman.models.Expense;
import com.simbiosyscorp.thetravelingsalesman.models.ExpenseContent;
import com.simbiosyscorp.thetravelingsalesman.models.Trip;
import com.simbiosyscorp.thetravelingsalesman.models.TripContent;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.UUID;

//Intent Extras
//request expense tab(trip is default), KEY: ORIGIN, NAME: "EXPENSE"
//set default client, KEY: CLIENT, NAME: client's UUID in String

public class TripExpMan extends NavigationDrawerActivity
        implements SectionsPagerAdapter.PlaceholderFragment.SectionNumberHolder,
        TripsListFragment.OnFragmentInteractionListener,
        ExpenseListFragment.OnFragmentInteractionListener {


    private static final String EXTRA_ORIGIN = "ORIGIN";
    private static final String EXTRA_CLIENT = "CLIENT";
    private static final String ORIGIN_TRIP = "TRIP";
    private static final String ORIGIN_EXPENSE = "EXPENSE";
    private static final String DEBUG_TAG = "TripExpMan";
    static String clientDefault = null;
    /**
     * The {@link ViewPager} that will host the section contents.
     */
    private static ViewPager mViewPager;
    LinearLayout contextChildView = null;
    int fragmentSection;
    /**
     * The {@link android.support.v4.view.PagerAdapter} that will provide
     * fragments for each of the sections. We use a
     * {@link FragmentPagerAdapter} derivative, which will keep every
     * loaded fragment in memory. If this becomes too memory intensive, it
     * may be best to switch to a
     * {@link android.support.v4.app.FragmentStatePagerAdapter}.
     */
    private SectionsPagerAdapter mSectionsPagerAdapter;

    public static Intent newTripIntent(Context context, UUID clientId) {
        Intent intent = new Intent(context, TripExpMan.class);
        intent.putExtra(EXTRA_ORIGIN, ORIGIN_TRIP);
        if (clientId != null) {
            intent.putExtra(EXTRA_CLIENT, clientId.toString());
        }

        return intent;

    }

    public static Intent newExpenseIntent(Context context, UUID clientId) {
        Intent intent = new Intent(context, TripExpMan.class);
        intent.putExtra(EXTRA_ORIGIN, ORIGIN_EXPENSE);
        if (clientId != null) {
            intent.putExtra(EXTRA_CLIENT, clientId.toString());
        }

        return intent;

    }

    public static void setCurrentTab(int item, Context context) {
        mViewPager.setCurrentItem(item);
        Toast.makeText(context, "I Tried", Toast.LENGTH_LONG).show();

    }

    public static int getCurrentItem() {
        return mViewPager.getCurrentItem();
    }

    @Override
    public void reportSection(int position) {

    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_trip_exp_man);
        ViewGroup appBar = (ViewGroup) findViewById(R.id.appbar);
        getLayoutInflater().inflate(R.layout.view_tab, appBar);

        setTitle("Trips and Expenses");


        // Create the adapter that will return a fragment for each of the three
        // primary sections of the activity.
        mSectionsPagerAdapter = new SectionsPagerAdapter(getSupportFragmentManager());

        // Set up the ViewPager with the sections adapter.
        mViewPager = (ViewPager) findViewById(R.id.container);
        mViewPager.setAdapter(mSectionsPagerAdapter);
        TabLayout tabLayout = (TabLayout) findViewById(R.id.tabLayout);
        tabLayout.setupWithViewPager(mViewPager);
        tabLayout.setOnTabSelectedListener(new TabLayout.ViewPagerOnTabSelectedListener(mViewPager) {
            @Override
            public void onTabSelected(TabLayout.Tab tab) {
                super.onTabSelected(tab);
                fragmentSection = tab.getPosition();
                if (fragmentSection == 0) {
                    checkMenu(R.id.nav_trip);
                } else {
                    checkMenu(R.id.nav_expenses);
                }

            }

            @Override
            public void onTabUnselected(TabLayout.Tab tab) {

            }

            @Override
            public void onTabReselected(TabLayout.Tab tab) {

            }
        });



        Log.i(DEBUG_TAG, "on create");

    }

    public void editIntent(Object obj) {
        if (obj instanceof Trip) {
            Intent intent = new Intent(getApplicationContext(), TravelDetail.class);
            intent.putExtra("TRIP", (Trip) obj);
            startActivity(intent);
            // Toast.makeText(getApplicationContext(),""+((Trip) obj).getId(),Toast.LENGTH_LONG).show();
        } else if (obj instanceof Expense) {
            Intent intent = new Intent(getApplicationContext(), ExpenseAdd.class);
            intent.putExtra("EXPENSE", (Expense) obj);
            startActivity(intent);
            //Toast.makeText(getApplicationContext(),""+((Expense) obj).getId(),Toast.LENGTH_LONG).show();
        }

    }

    public void removeUtil(Object obj) {

        TripContent tripContent = TripContent.get(getApplicationContext());
        ExpenseContent expenseContent = ExpenseContent.get(getApplicationContext());
        if (obj instanceof Trip) {
            tripContent.delete(((Trip) obj).getId());
            Toast.makeText(getApplicationContext(), "" + ((Trip) obj).getId(), Toast.LENGTH_LONG).show();
        } else if (obj instanceof Expense) {
            expenseContent.delete(((Expense) obj).getId());
            Toast.makeText(getApplicationContext(), "" + ((Expense) obj).getId(), Toast.LENGTH_LONG).show();
        }


    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        //getMenuInflater().inflate(R.menu.menu_trip_exp_man, menu);
        menu.add(1, 66, 100, "ADD");
        //menu.findItem(11).setIcon(android.R.drawable.ic_menu_save);
        menu.findItem(66).setIcon(R.drawable.ic_add_white_24dp);
        menu.findItem(66).setShowAsAction(MenuItem.SHOW_AS_ACTION_ALWAYS);
        return true;
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        // Handle action bar item clicks here. The action bar will
        // automatically handle clicks on the Home/Up button, so long
        // as you specify a parent activity in AndroidManifest.xml.
        int id = item.getItemId();

        //noinspection SimplifiableIfStatement
        if (id == R.id.action_settings) {
            return true;
        }
        if (id == 66) {

            //SectionsPagerAdapter.PlaceholderFragment placeholderFragment = (SectionsPagerAdapter.PlaceholderFragment) getSupportFragmentManager().findFragmentById(R.id.fragmentLinearLayout);


            if (fragmentSection == 0) {
                Intent intent = new Intent(getApplicationContext(), TravelDetail.class);
                if (SectionsPagerAdapter.PlaceholderFragment.selection != null && !SectionsPagerAdapter.PlaceholderFragment.selection.getFirstName().equals("All"))
                    intent.putExtra("CLIENT", SectionsPagerAdapter.PlaceholderFragment.selection.getId().toString());
                startActivity(intent);
            } else {
                Intent intent = new Intent(getApplicationContext(), ExpenseAdd.class);
                if (SectionsPagerAdapter.PlaceholderFragment.selection != null && !SectionsPagerAdapter.PlaceholderFragment.selection.getFirstName().equals("All"))
                    intent.putExtra("CLIENT", SectionsPagerAdapter.PlaceholderFragment.selection.getId().toString());
                startActivity(intent);
            }

            return true;
        }

        return super.onOptionsItemSelected(item);
    }

    @Override
    public void onCreateContextMenu(ContextMenu menu, View v, ContextMenu.ContextMenuInfo menuInfo) {
        super.onCreateContextMenu(menu, v, menuInfo);
        int groupId = 1;
        menu.add(groupId, 11, 100, "Edit");
        menu.add(groupId, 22, 200, "Delete");
        contextChildView = (LinearLayout) v;
    }

    @Override
    public boolean onContextItemSelected(MenuItem item) {


        int id = item.getItemId();

        switch (id) {

            case 11: {

                editIntent(contextChildView.getTag());
                Toast.makeText(getApplicationContext(), "Edit", Toast.LENGTH_SHORT).show();
                return true;
            }

            case 22: {
                // SectionsPagerAdapter.PlaceholderFragment placeholderFragment = (SectionsPagerAdapter.PlaceholderFragment)getSupportFragmentManager().findFragmentById(R.id.fragmentLinearLayout);
                // if(placeholderFragment != null){
                removeUtil(contextChildView.getTag());

                LinearLayout linear = (LinearLayout) contextChildView.getParent();
                linear.removeView(contextChildView);
                Toast.makeText(getApplicationContext(), "Delete", Toast.LENGTH_SHORT).show();
                return true;
            }

        }
        return super.onContextItemSelected(item);


    }

    @Override
    protected void onNewIntent(Intent intent) {
        super.onNewIntent(intent);
        setIntent(intent);
    }

    @Override
    protected void onResume() {
        super.onResume();
        setUp();
    }

    private void setUp() {
        Log.i(DEBUG_TAG, "set up");
        String request = null;
        Intent incoming = getIntent();
        if (incoming != null && incoming.hasExtra("ORIGIN")) {
            request = incoming.getStringExtra("ORIGIN");
            getIntent().removeExtra("ORIGIN");
        }
        if (incoming != null && incoming.hasExtra("CLIENT"))
            clientDefault = incoming.getStringExtra("CLIENT");

        if (request != null && request.equals("EXPENSE")) {
            mViewPager.setCurrentItem(1); //expense tab
            //Toast.makeText(getApplicationContext(),"expense", Toast.LENGTH_LONG).show();
        } else if (request != null)
            mViewPager.setCurrentItem(0);

        if (fragmentSection == 0)
            checkMenu(R.id.nav_trip);
        else
            checkMenu(R.id.nav_expenses);
    }


    @Override
    public void onFragmentInteraction(View v) {
        registerForContextMenu(v);
    }
}

/**
 * A {@link FragmentPagerAdapter} that returns a fragment corresponding to
 * one of the sections/tabs/pages.
 */
class SectionsPagerAdapter extends FragmentPagerAdapter {

    public SectionsPagerAdapter(FragmentManager fm) {
        super(fm);
    }

    @Override
    public Fragment getItem(int position) {
        // getItem is called to instantiate the fragment for the given page.
        // Return a PlaceholderFragment (defined as a static inner class below).
        return PlaceholderFragment.newInstance(position + 1);
    }

    @Override
    public int getCount() {
        // Show 3 total pages.
        return 2;
    }

    @Override
    public CharSequence getPageTitle(int position) {
        switch (position) {
            case 0:
                return "Trip";
            case 1:
                return "Expenses";
        }
        return null;
    }


    public static class PlaceholderFragment extends Fragment {

        private static final String DEBUG_TAG = "PlaceholderFragment";
        private static final int REQUEST_CLIENT = 2;
        private static final String ARG_SECTION_NUMBER = "section_number";
        static Client selection = null;
        /**
         * The fragment argument representing the section number for this
         * fragment.
         */
        SectionNumberHolder mCallback;
        int currentTab;
        //AutoCompleteTextView autoCompleteTextView;
        TextView dateSet;
        ClientManager clientManager;
        List<Client> clientList;
        List<Object> list;
        boolean tripBool;
        ExpenseContent expenseContent;
        boolean date_not_set = true;
        View view_client;
        TextView clientNameView;
        Button selectClient;
        TripContent tripContent;
        LinearLayout linearLayout;
        Calendar date = Calendar.getInstance();


        ImageButton button;
        ImageButton clearClient;
        public PlaceholderFragment() {
        }

        /**
         * Returns a new instance of this fragment for the given section
         * number.
         */
        public static PlaceholderFragment newInstance(int sectionNumber) {
            PlaceholderFragment fragment = new PlaceholderFragment();
            Bundle args = new Bundle();
            args.putInt(ARG_SECTION_NUMBER, sectionNumber);
            fragment.setArguments(args);
            return fragment;
        }

        @Override
        public void onCreate(Bundle savedInstanceState) {
            super.onCreate(savedInstanceState);
            clientManager = ClientManager.get(getActivity());
            if(TripExpMan.clientDefault != null
                        && TripCursorWrapper.isUUIDValid(TripExpMan.clientDefault)) {

                    selection = clientManager.getClient(UUID.fromString(TripExpMan.clientDefault));
            }
        }

        @Override
        public View onCreateView(LayoutInflater inflater, ViewGroup container,
                                 Bundle savedInstanceState) {
            View rootView = inflater.inflate(R.layout.fragment_trip_exp_man, container, false);
            tripBool = true;
            try {
                mCallback = (SectionNumberHolder) getActivity();
            } catch (ClassCastException e) {
                throw new ClassCastException(getActivity().toString()
                        + " must implement SectionNumberHolder");
            }

            dateSet = (TextView) rootView.findViewById(R.id.expensesDateText);
            button = (ImageButton) rootView.findViewById(R.id.fragmentCalenderButton);

            selectClient = (Button) rootView.findViewById(R.id.selectClient_trip_exp_man);
            //autoCompleteTextView = (AutoCompleteTextView) rootView.findViewById(R.id.tripExpManAutoClient);
            linearLayout = (LinearLayout) rootView.findViewById(R.id.fragmentLinearListLayout);
            clearClient = (ImageButton) rootView.findViewById(R.id.button_clear);

            //mCallback.reportSection(getArguments().getInt(ARG_SECTION_NUMBER));

                /*clientList = clientManager.getClients();
                Client clientAll = new Client();
                clientAll.setFirstName("All");
                clientAll.setLastName("");
                ArrayAdapter<Client> adapter = new ArrayAdapter<Client>(getActivity(), android.R.layout.simple_dropdown_item_1line, clientList);
                adapter.add(clientAll);
                //autoCompleteTextView.setAdapter(adapter);
                */
            expenseContent = ExpenseContent.get(getActivity());

            tripContent = TripContent.get(getActivity());

            selectClient.setOnClickListener(new View.OnClickListener() {
                @Override
                public void onClick(View v) {
                    currentTab = TripExpMan.getCurrentItem();
                    startActivityForResult(new Intent(getContext(), ClientPickActivity.class),
                            REQUEST_CLIENT);
                }
            });

            button.setOnClickListener(new View.OnClickListener() {
                @Override
                public void onClick(View v) {
                    android.support.v4.app.FragmentTransaction ft = getFragmentManager().beginTransaction();
                    DatePickerFragment datePickerFragment = DatePickerFragment.newInstance(date);
                    datePickerFragment.setTargetFragment(PlaceholderFragment.this, 0);
                    ft.add(datePickerFragment, null);
                    ft.commit();

                }
            });

            clearClient.setOnClickListener(new View.OnClickListener() {
                @Override
                public void onClick(View v) {
                    selection = null;
                    updateClient();
                    display();
                }
            });


                /*
                autoCompleteTextView.setOnItemClickListener(new AdapterView.OnItemClickListener() {
                    @Override
                    public void onItemClick(AdapterView<?> parent, View view, int position, long id) {
                        selection= (Client)parent.getItemAtPosition(position);
                        linearLayout.removeAllViews();
                        display();
                    }
                });

*/

            return rootView;
        }

        // format("%tD", cal);
        public void onActivityResult(int request_code, int resultCode, Intent intent) {
            if (intent != null) {
                if (request_code == REQUEST_CLIENT) {
                    // TripExpMan.setCurrentTab(currentTab,getActivity());
                    UUID uuid = UUID.fromString(intent.getStringExtra(ClientPickActivity.EXTRA_CLIENT_ID));
                    selection = ClientManager.get(getContext()).getClient(uuid);

                }
//                else {
//                    Bundle bundle;
//                    bundle = intent.getExtras();
//                    date = (Calendar) bundle.get("com.appers.avyaz.thetravelingsalesman.task.date");
//                    date_not_set = false;
//                    linearLayout.removeAllViews();
//                    display();
//                }
            }
        }

        @Override
        public void onResume() {
            super.onResume();
            Log.i(DEBUG_TAG, "onresume");

//                if(TripExpMan.clientDefault != null
//                        && TripCursorWrapper.isUUIDValid(TripExpMan.clientDefault)) {
//                    selection = clientManager.getClient(UUID.fromString(TripExpMan.clientDefault));
//                }

            updateClient();


//                }
            display();
        }

        private void updateClient() {
            if (selection != null) {
                selectClient.setText(selection.toString());
                clearClient.setVisibility(View.VISIBLE);
            } else {
                selectClient.setText(R.string.client_not_selected);
                clearClient.setVisibility(View.GONE);
            }
        }

        public void display() {
            linearLayout.removeAllViews();

            if (!date_not_set) {
                dateSet.setText(String.format("%tm/%td/%tY", date, date, date));
            } else {
                dateSet.setText("--/--/----");
            }

            if (selection == null || selection.getFirstName().equals("All")) {
                if (getArguments().getInt(ARG_SECTION_NUMBER) == 1) {
                    list = new ArrayList<Object>(tripContent.getTrips());
                    tripBool = true;
                } else {
                    list = new ArrayList<Object>(expenseContent.getExpenses());
                    tripBool = false;
                }
            } else {
                if (getArguments().getInt(ARG_SECTION_NUMBER) == 1) {
                    list = new ArrayList<Object>(tripContent.getClientTrips(selection.getId()));
                    tripBool = true;
                } else {
                    list = new ArrayList<Object>(expenseContent.getClientExpenses(selection.getId()));
                    tripBool = false;
                }


            }
            int i = 0;
            while (i < list.size()) {
                if (tripBool) {
                    Trip trip = (Trip) list.get(i);
                    if (!date_not_set && TripContent.compareCalendars(trip.getDate_from(), date) == -1) {
                        i++;
                        continue;
                    }
                    FragmentManager fragmentManager = getChildFragmentManager();
                    android.support.v4.app.FragmentTransaction fragmentTransaction = fragmentManager.beginTransaction();
                    TripsListFragment tripsListFragment = TripsListFragment.newInstance(trip);
                    fragmentTransaction.add(R.id.fragmentLinearListLayout, tripsListFragment).commit();

                } else {
                    Expense expense = (Expense) list.get(i);
                    if (!date_not_set && TripContent.compareCalendars(expense.getDate_from(), date) == -1) {
                        i++;
                        continue;
                    }
                    FragmentManager fragmentManager = getChildFragmentManager();
                    android.support.v4.app.FragmentTransaction fragmentTransaction = fragmentManager.beginTransaction();
                    ExpenseListFragment expenseListFragment = ExpenseListFragment.newInstance(expense);
                    fragmentTransaction.add(R.id.fragmentLinearListLayout, expenseListFragment).commit();

                }

                i++;
            }


        }

        public interface SectionNumberHolder {
            public void reportSection(int position);
        }

    }


}

