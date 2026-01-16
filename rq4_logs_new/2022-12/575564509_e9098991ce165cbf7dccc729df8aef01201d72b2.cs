using System.Data;
using UnityEngine;

public class CarsharingDB : MonoBehaviour
{
    #region Methods

    #region Public Methods

    public static string GetUsername(string name)
    {
        var username = SQLManager.ExecuteQueryWithAnswer(QUERY_GET_USERNAME(name));
        return (username == "") ? "" : username;
    }

    public static void CreateNewUser(string name)
    {
        SQLManager.ExecuteQueryWithoutAnswer(QUERY_CREATE_USER(name));
    }

    public static DataTable GetProfileRecords(string name)
    {
        var data = SQLManager.GetTable(QUERY_GET_PROFILE_RECORDS(name));
        return data;
    }

    public static DataTable GetSpecialists()
    {
        var data = SQLManager.GetTable(QUERY_GET_SPECIALISTS());
        return data;
    }

    public static DataTable GetTimes(string spec, string date)
    {
        var data = SQLManager.GetTable(QUERY_GET_TIMES(spec, date));
        return data;
    }

    public static void SetNewRecord(string user, string spec, string date, string time)
    {
        SQLManager.ExecuteQueryWithoutAnswer(QUERY_SET_NEW_RECORD(user, spec, date, time));
        SQLManager.ExecuteQueryWithoutAnswer(QUERY_SET_NEW_CARD(user, date, time));
    }

    #endregion

    #region Private Methods

    private static string QUERY_GET_USERNAME(string name)
    {
        return "SELECT patient_FIO FROM Patient WHERE patient_FIO = '"+name+"';";
    }

    private static string QUERY_CREATE_USER(string name)
    {
        return "INSERT INTO Patient(patient_FIO, patient_room_id) VALUES ('"+name+"', NULL);";
    }   
    
    private static string QUERY_GET_PROFILE_RECORDS(string name)
    {
        return "SELECT Card.card_session_result, Session.session_doctor_FIO, Doctor.doctor_speciality, " +
               "Doctor.doctor_cabinet, Session.session_date, Session.session_time FROM Card, Session, Doctor WHERE Card.card_session_id = " +
               "Session.session_id AND Session.session_doctor_FIO = Doctor.doctor_FIO AND Card.card_patient_FIO = '"+name+"';";
    }

    private static string QUERY_GET_SPECIALISTS()
    {
        return "SELECT doctor_speciality FROM Doctor;";
    }
      
    private static string QUERY_GET_TIMES(string speciality, string date)
    {
        return "SELECT session_time FROM Session WHERE session_doctor_FIO = (SELECT doctor_FIO FROM Doctor WHERE doctor_speciality = '"+speciality+"') AND session_date = '"+date+"';";
    }   
    
    private static string QUERY_SET_NEW_RECORD(string user, string speciality, string date, string time)
    {
        return "INSERT INTO Session('session_patient_FIO','session_doctor_FIO','session_date','session_time') VALUES('"+user+"', (SELECT doctor_FIO FROM Doctor WHERE doctor_speciality = '"+speciality+"'), '"+date+"', '"+time+"');";
    } 
    
    private static string QUERY_SET_NEW_CARD(string user, string date, string time)
    {
        return "INSERT INTO Card('card_patient_FIO', 'card_session_id', 'card_session_result') VALUES('"+user+"', (SELECT session_id FROM Session WHERE session_date = '"+date+"' AND session_time = '"+time+"'), 'Данных нет');";
    }



    #endregion

    #endregion
}