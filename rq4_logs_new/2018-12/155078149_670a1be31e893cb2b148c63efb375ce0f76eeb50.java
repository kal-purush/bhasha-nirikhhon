package Controller;

import Model.*;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.scene.control.Alert;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class LogedInController {

    SQLModel sqlModel;
    User loged;

    public LogedInController() {
        this.sqlModel = SQLModel.GetInstance();

    }

    public boolean tryLogIn(String username,String pwd){
        String[] fields = new String[TblFields.enumDict.get("userFields").size()];
        fields[0] = username;
        fields[1] = pwd;
        String user = sqlModel.selectFromTable(Tables.TBL_USERS, fields);
        if(user == ""){
            return false;
        }
        String[] valid = user.split("\n");
        loged = new User(valid[0]);
        return true;
    }

    public ObservableList<VactaionAndRequest> getMyRequests(){
//        String[] fields = new String[TblFields.enumDict.get("requestTblFields").size()];
//        fields[1] = logedInUsername;
//        String[] allRequests = SQLModel.GetInstance().selectFromTable(Tables.TBL_REQUESTS, fields).split("\n");
        ArrayList<Request> allRequests = loged.getMyRequests();
        List<VactaionAndRequest> vactaionAndRequests = new ArrayList<>();
        for (int i = 0; i < allRequests.size(); i++) {
            Request req = allRequests.get(i);
            Vacation vac = req.getVacation();
            if (vac.get_vacationStatus() == Vacation.VacationStatus.IN_PROGRESS) {
                vactaionAndRequests.add(new VactaionAndRequest(new SimpleDateFormat("dd/mm/yyyy-HH:mm").format(vac.get__startDate()),
                        new SimpleDateFormat("dd/MM/yyyy-HH:mm").format(vac.get_endDate()),
                        vac.get_destination(), loged.getUsername(), req.getR_answer()));
            }
        }
//            vactaionAndRequests.add(new VactaionAndRequest(new SimpleDateFormat("dd/mm/yyyy-HH:mm").format(vac.get__startDate()),
//                    new SimpleDateFormat("dd/mm/yyyy-HH:mm").format(vac.get_endDate()), vac.get_destination(), logedInUsername, req.getR_answer()));
//            VactaionAndRequest vactaionAndRequest =ne
//            Request req = new Request(allRequests[i]);
//            String vecationID = req.getVacationID();
//            fields = new String[TblFields.enumDict.get("vacationFields").size()];
//            fields[0] = vecationID;
//            fields[10] = Vacation.VacationStatus.IN_PROGRESS.name();
//            String reqVacation = SQLModel.GetInstance().selectFromTable(Tables.TBL_VACATIONS, fields);
//            if (reqVacation != "") {
//                Vacation vac = new Vacation(reqVacation);
//                DateFormat formatter = new SimpleDateFormat("dd/mm/yyyy-HH:mm");
//
//            }

       // }
        return FXCollections.observableList(vactaionAndRequests);
    }

    public ObservableList<VactaionAndRequest> getToAnswerRequests(){
        ArrayList<Request> allRequests = loged.getRequestsForMe();
//        String[] fields = new String[TblFields.enumDict.get("requestTblFields").size()];
//        fields[2] = logedInUsername;
//        fields[4] = "pending";
//        String[] allRequests = SQLModel.GetInstance().selectFromTable(Tables.TBL_REQUESTS, fields).split("\n");
        List<VactaionAndRequest> vactaionAndRequests = new ArrayList<>();
        for (int i = 0; i < allRequests.size(); i++) {
            Request req = allRequests.get(i);
            if(allRequests.get(i).getR_answer() == "pending"){
                Vacation vac = req.getVacation();
                vactaionAndRequests.add(new VactaionAndRequest(new SimpleDateFormat("dd/MM/yyyy-HH:mm").format(vac.get__startDate()),
                        new SimpleDateFormat("dd/MM/yyyy-HH:mm").format(vac.get_endDate()),
                        vac.get_destination(), loged.getUsername(), req.getR_answer()));
            }
//            Request req = new Request(allRequests[i]);
//            String vecationID = req.getVacationID();
//            fields = new String[TblFields.enumDict.get("vacationFields").size()];
//            fields[0] = vecationID;
            //String reqVacation = SQLModel.GetInstance().selectFromTable(Tables.TBL_VACATIONS, fields);
//            if (reqVacation != "") {
//                Vacation vac = new Vacation(reqVacation);
//                DateFormat formatter = new SimpleDateFormat("dd/mm/yyyy-HH:mm");
//                vactaionAndRequests.add(new VactaionAndRequest(new SimpleDateFormat("dd/mm/yyyy-HH:mm:ss").format(vac.get__startDate()),
//                        new SimpleDateFormat("dd/mm/yyyy-HH:mm:ss").format(vac.get_endDate()), vac.get_destination(), logedInUsername, req.getR_answer()));
//            }
        }
        return FXCollections.observableList(vactaionAndRequests);
    }

    public void deleteUser(){
        ISQLable userToDelete = new User(loged.getUsername(), "",null,"",
                "","","","");
        sqlModel.deleteRecordFromTable(userToDelete);
        loged = null;
    }

    public void UpdateUser( String pwd, Date birthday, String privateName, String lastName,
                           String city, String bankAcount, String creditCard, String id){
        loged = new User(loged.getUsername(), pwd, birthday, privateName, lastName, city, bankAcount, id);
        sqlModel.updateRecord(loged);
    }

    public void CreateVacation(Date __startDate, Date _endDate,
                               String _destination, String _aviationCompany,
                               int _numOfTickets, Vacation.TicketType _ticketType,
                               boolean _isBaggageIncluded, boolean _isRoundTrip,
                               Vacation.VacationType _vacationType, Vacation.VacationStatus _vacationStatus,
                               Vacation.VacationSleepingArrangements _vacationSleepingArrangements, String _ownerID){
        ISQLable newVacation = new Vacation(loged.getUsername(),__startDate, _endDate, _destination, _aviationCompany, _numOfTickets, _ticketType, _isBaggageIncluded, _isRoundTrip, _vacationType, _vacationStatus, _vacationSleepingArrangements, _ownerID);
        sqlModel.insertRecordToTable(Tables.TBL_VACATIONS.toString().toLowerCase(), newVacation);
    }

    public void CreateRequestAndUpdateVacation( String r_seller, String vacationID) {
        Request newRequest = new Request(loged.getUsername(), r_seller, vacationID);
        //create request
        sqlModel.insertRecordToTable(Tables.TBL_REQUESTS.name().toLowerCase(),newRequest);
        loged.UpdateMyRequsts(newRequest);
        //update vacation
        newRequest.getVacation().set_vacationStatus(Vacation.VacationStatus.IN_PROGRESS);
        sqlModel.updateRecord(newRequest.getVacation());
//        String[] fields = {newRequest.getR_ID(),newRequest.getR_seller(),"","","","",
//                "","","","","","",""};
//        String vacation = sqlModel.selectFromTable(Tables.TBL_VACATIONS,fields);
//        if(vacation!=""){
//            Vacation forUpdate = new Vacation(vacation);
//            forUpdate.set_vacationStatus(Vacation.VacationStatus.IN_PROGRESS);
//            sqlModel.updateRecord(forUpdate);
//        }
    }

    public void CreatePaymentAndUpdateVacation(String aprovedRequestId){
        Payments newPayment = new Payments(aprovedRequestId);

        newPayment.get_Request().setR_answer("Approved");
        newPayment.get_Request().setPayment(newPayment);
        loged.UpdateMyRequsts(newPayment.get_Request());
        sqlModel.updateRecord(newPayment.get_Request());
        newPayment.get_Request().getVacation().set_vacationStatus(Vacation.VacationStatus.SOLD);
        sqlModel.updateRecord(newPayment.get_Request().getVacation());
        //Insert Payment to DB
        sqlModel.insertRecordToTable(Tables.TBL_PAYMENTS.name().toLowerCase(), newPayment);

        //Set request as approved:
        //String[] requestFields = new String[TblFields.enumDict.get("requestTblFields").size()];
        //String approvedReqId = newPayment.get_aprovedRequest();
        //requestFields[0] = approvedReqId;
        //approvedRequest.setPayment(newPayment);
        //Request approvedRequest = new Request(sqlModel.selectFromTable(Tables.TBL_REQUESTS, requestFields));
        //approvedRequest.setR_answer("Approved");
        //Set vacation as sold:
//        String[] vacationFields = new String[TblFields.enumDict.get("vacationFields").size()];
//        String vacationId = approvedRequest.getVacationID();
//        vacationFields[0] = vacationId;

        //Vacation vacation = new Vacation(sqlModel.selectFromTable(Tables.TBL_VACATIONS, vacationFields));
        // vacation.set_vacationStatus(Vacation.VacationStatus.SOLD);
    }
}