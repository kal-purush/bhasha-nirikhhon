using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DataAccess.Base;
using Interface.Service;
using System.Data;
using DataAccess.Entity;
using DataAccess.Common;

namespace BusinessProcess.Service
{
    public class BCommonData : ProcessBase, ICommonData
    {
        ClsUtility oUtility = new ClsUtility();

        
        public BCommonData() { }

        public DataTable getUserList()
        {
            oUtility.Init_Hashtable();
            ClsObject oUserList = new ClsObject();
            //return (DataTable)oUserList.ReturnObject(oUtility.theParams, "VW_UserDesignationTransaction", oUtility.ObjectEnum.DataTable);
            return (DataTable)oUserList.ReturnObject(oUtility.theParams, "select * from VW_UserDesignationTransaction", ClsUtility.ObjectEnum.DataTable);// Change by Rahmat(14.02.17)
        }

        public DataTable getMSTDecode(Int32 CodeID)
        {
            oUtility.Init_Hashtable();
            ClsObject oMstCodeDecode = new ClsObject();
            oUtility.AddParameters("@CodeID", SqlDbType.Int, CodeID.ToString());

            return (DataTable)oMstCodeDecode.ReturnObject(oUtility.theParams, "sp_GetMSTDecode", ClsUtility.ObjectEnum.DataTable);
        }

        public DataTable getAllMSTDecodeIn(string Codes)
        {
            oUtility.Init_Hashtable();
            ClsObject oMstCodeDecode = new ClsObject();
            oUtility.AddParameters("@Codes", SqlDbType.NVarChar, Codes.ToString());

            return (DataTable)oMstCodeDecode.ReturnObject(oUtility.theParams, "sp_GetAllMSTDecode", ClsUtility.ObjectEnum.DataTable);
        }

        public DataTable getAllMSTCode()
        {
            oUtility.Init_Hashtable();
            ClsObject oMstAllCode = new ClsObject();

            return (DataTable)oMstAllCode.ReturnObject(oUtility.theParams, "sp_GetAllMSTCode", ClsUtility.ObjectEnum.DataTable);
        }

        public DataTable getAllEmployees()
        {
            oUtility.Init_Hashtable();
            ClsObject oMstAllEmployees = new ClsObject();

            return (DataTable)oMstAllEmployees.ReturnObject(oUtility.theParams, "sp_GetAllEmployees", ClsUtility.ObjectEnum.DataTable);
        }

        public DataTable getAllDesignation()
        {
            oUtility.Init_Hashtable();
            ClsObject oMstAllDesignation = new ClsObject();

            return (DataTable)oMstAllDesignation.ReturnObject(oUtility.theParams, "sp_GetAllDesignation", ClsUtility.ObjectEnum.DataTable);
        }


        public DataTable getAllLPTF()
        {
            oUtility.Init_Hashtable();
            ClsObject oMstGetAllLPTF = new ClsObject();

            return (DataTable)oMstGetAllLPTF.ReturnObject(oUtility.theParams, "sp_GetAllLPTF", ClsUtility.ObjectEnum.DataTable);
        }

        public DataTable getAllHIVDiseases()
        {
            oUtility.Init_Hashtable();
            ClsObject oMstAllHIVDiseases = new ClsObject();

            return (DataTable)oMstAllHIVDiseases.ReturnObject(oUtility.theParams, "sp_GetAllHIVDiseases", ClsUtility.ObjectEnum.DataTable);
        }

        public DataTable getAllSymptoms()
        {
            oUtility.Init_Hashtable();
            ClsObject oMstAllSymptoms = new ClsObject();

            return (DataTable)oMstAllSymptoms.ReturnObject(oUtility.theParams, "sp_GetAllMSTSymptoms", ClsUtility.ObjectEnum.DataTable);
        }

        public DataTable getAllReasons()
        {
            oUtility.Init_Hashtable();
            ClsObject oMstAllReasons = new ClsObject();

            return (DataTable)oMstAllReasons.ReturnObject(oUtility.theParams, "sp_GetAllReasons", ClsUtility.ObjectEnum.DataTable);
        }

        public DataTable getAllWards()
        {
            oUtility.Init_Hashtable();
            ClsObject oMstAllWards = new ClsObject();

            return (DataTable)oMstAllWards.ReturnObject(oUtility.theParams, "sp_GetAllWards", ClsUtility.ObjectEnum.DataTable);
        }

        public DataTable getAllVillages()
        {
            oUtility.Init_Hashtable();
            ClsObject oMstAllVillages = new ClsObject();

            return (DataTable)oMstAllVillages.ReturnObject(oUtility.theParams, "sp_GetAllVillages", ClsUtility.ObjectEnum.DataTable);
        }

        public DataTable getAllCouncellingType()
        {
            oUtility.Init_Hashtable();
            ClsObject oMstAllCounsellingType = new ClsObject();

            return (DataTable)oMstAllCounsellingType.ReturnObject(oUtility.theParams, "sp_GetAllCounsellingType", ClsUtility.ObjectEnum.DataTable);
        }

        public DataTable getAllCouncellingTopic()
        {
            oUtility.Init_Hashtable();
            ClsObject oMstAllCounsellingTopic = new ClsObject();

            return (DataTable)oMstAllCounsellingTopic.ReturnObject(oUtility.theParams, "sp_GetAllCouncellingTopic", ClsUtility.ObjectEnum.DataTable);
        }

        public DataTable getAllDistrict()
        {
            oUtility.Init_Hashtable();
            ClsObject oMstAllDistrict = new ClsObject();

            return (DataTable)oMstAllDistrict.ReturnObject(oUtility.theParams, "sp_GetAllDistrict", ClsUtility.ObjectEnum.DataTable);
        }

        public DataTable getAllProvince()
        {
            oUtility.Init_Hashtable();
            ClsObject oMstAllProvince = new ClsObject();

            return (DataTable)oMstAllProvince.ReturnObject(oUtility.theParams, "sp_GetAllProvince", ClsUtility.ObjectEnum.DataTable);
        }

        public DataTable getAllCountries()
        {
            oUtility.Init_Hashtable();
            ClsObject oMstAllCountries = new ClsObject();

            return (DataTable)oMstAllCountries.ReturnObject(oUtility.theParams, "sp_GetAllCountries", ClsUtility.ObjectEnum.DataTable);
        }

        public DataTable getAllEducation()
        {
            oUtility.Init_Hashtable();
            ClsObject oMstAllEducation = new ClsObject();

            return (DataTable)oMstAllEducation.ReturnObject(oUtility.theParams, "sp_GetAllEducation", ClsUtility.ObjectEnum.DataTable);
        }

        public DataTable getAllARTSponsor()
        {
            oUtility.Init_Hashtable();
            ClsObject oMstARTSponsor = new ClsObject();

            return (DataTable)oMstARTSponsor.ReturnObject(oUtility.theParams, "sp_GetAllARTSponsor", ClsUtility.ObjectEnum.DataTable);
        }

        public DataTable getAllHivDisclosure()
        {
            oUtility.Init_Hashtable();
            ClsObject oMstHivDisclosure = new ClsObject();

            return (DataTable)oMstHivDisclosure.ReturnObject(oUtility.theParams, "sp_GetAllHivDisclosure", ClsUtility.ObjectEnum.DataTable);
        }

        public DataTable getAllDivision()
        {
            oUtility.Init_Hashtable();
            ClsObject oMstDivision = new ClsObject();

            return (DataTable)oMstDivision.ReturnObject(oUtility.theParams, "sp_GetAllDivision", ClsUtility.ObjectEnum.DataTable);
        }


    }
    
}