package it.polito.mad.data_layer_access;

import com.firebase.geofire.GeoFire;
import com.google.android.gms.location.FusedLocationProviderClient;
import com.google.firebase.auth.FirebaseAuth;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;

public class FirebaseUtils {

    //    Common among all the three modules
    public static FirebaseAuth auth;
    public static DatabaseReference database;
    public static String Uid;


    //    Restaurant's DatabaseReferences

    public static DatabaseReference timeBranch;
    public static DatabaseReference popularFoodBranch;
    public static DatabaseReference branchRestaurantOrders;
    public static DatabaseReference branchOrdersFlag;
    public static DatabaseReference branchOrdersIncoming;
    public static DatabaseReference branchOrdersInPreparation;
    public static DatabaseReference branchOrdersReady;
    public static DatabaseReference branchDailyFood;
    public static DatabaseReference branchFavouriteFood;
    public static DatabaseReference branchCustomer;
    public static DatabaseReference branchRestaurantProfile;
    public static DatabaseReference branchDeliveryMan;
    public static DatabaseReference branchRestaurantTypeFood;

    public static GeoFire geofireRider;
    public static GeoFire geofireRestaurant;
    public static FusedLocationProviderClient mFusedLocationRestaurant;


    //        Customer's DatabaseReferences


    //        DeliverMan's DatabaseReferences
    public static void setupFirebase() {

//        Common among all the three modules

        auth = FirebaseAuth.getInstance();
        database = FirebaseDatabase.getInstance().getReference();
        Uid = auth.getCurrentUser().getUid();


//        Restaurant's DatabaseReferences

        branchRestaurantProfile = database.child("restaurants/" + Uid + "/Profile");
        branchRestaurantTypeFood = database.child("restaurants/" + Uid + "/type_food");
        branchCustomer = database.child("customers");
        branchDeliveryMan = database.child("delivery");
        timeBranch = database.child("restaurants/" + Uid + "/Time");
        popularFoodBranch = database.child("restaurants/" + Uid + "/Food_Analytics");
        branchDailyFood = database.child("restaurants/" + Uid + "/Daily_Food");
        branchFavouriteFood = database.child("restaurants/" + Uid + "/Favourites_Food/");
        branchRestaurantOrders = database.child("restaurants/" + Uid + "/Orders/");
        branchOrdersFlag = database.child("restaurants/" + Uid + "/Orders/IncomingReservationFlag");
        branchOrdersIncoming = database.child("restaurants/" + Uid + "/Orders/Incoming");
        branchOrdersInPreparation = database.child("restaurants/" + Uid + "/Orders/In_Preparation");
        branchOrdersReady = database.child("restaurants/" + Uid + "/Orders/Ready_To_Go");

        geofireRider = new GeoFire(database.child("riders_position"));
        geofireRestaurant = new GeoFire(database.child("restaurants_position"));
//        Customer's DatabaseReferences


//        DeliverMan's DatabaseReferences

    }
}