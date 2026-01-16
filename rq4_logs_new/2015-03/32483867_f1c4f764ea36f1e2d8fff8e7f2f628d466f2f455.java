package edu.sru.thangiah.zeus.tr.TRSolutionHierarchy.Heuristics.Insertion;

import edu.sru.thangiah.zeus.tr.TRSolutionHierarchy.*;

/**
 * Created by joshuasarver on 3/15/15.
 */
public class TRLinearLowestDistanceInsertion {

    private TRShipment insertMe;
    private TRDepotsList depotsList;

    //It is a static method so that it can be accessed without creating an object
    public static String WhoAmI() {
        return ("Insertion Type: TRGreedyInsertionRevised");
    }//END WHO_AM_I *******************<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


//METHOD-GETTER
//gets the location to insert the passed shipment


    /**
     * @aaron comment
     */
    public boolean getInsertShipment(TRDepotsList depotsList, TRShipment insertMe) {
        this.insertMe = insertMe;
        this.depotsList = depotsList;

        stepThroughDepots(depotsList);

        return true;
    }


    private void stepThroughDepots(TRDepotsList depotsList) {
        TRDepot theDepot = depotsList.getFirst();

        while (theDepot != depotsList.getTail()) {
            stepThroughTrucks(theDepot.getSubList());
            theDepot = theDepot.getNext();
        }
    }


    private void stepThroughTrucks(TRTrucksList trucksList) {
        TRTruck theTruck = trucksList.getFirst();

        while (theTruck != trucksList.getTail()) {
            stepThroughDays(theTruck.getSubList());
            theTruck = theTruck.getNext();
        }
    }

    private void stepThroughDays(TRDaysList daysList) {

        for(int i = 0; i < daysList.getSize(); i++){
            if(insertMe.getDaysVisited()[i]){
                stepThroughNodes((TRNodesList) daysList.getAtIndex(i).getSubList());
            }
        }
    }


    private void stepThroughNodes(TRNodesList nodesList){
        TRNode theNode = nodesList.getFirst();

        for(int i = 0; i < nodesList.getSize(); i++){
//            nodesList.getAtIndex(i).insertAfterCurrent();
        }

    }

    private void findLowestDistanceNodeInsertion(TRNodesList nodesList){

    }

    private void appendShipment(TRNodesList nodesList){
        nodesList.insertShipment(insertMe);
    }

//    private void stepThroughNodes(TRNodesList nodesList) {
//        TRNode theNode = nodesList.getFirst();
//
//        while (theNode != nodesList.getTail()) {
//
//            theNode = theNode.getNext();
//        }
//    }


}