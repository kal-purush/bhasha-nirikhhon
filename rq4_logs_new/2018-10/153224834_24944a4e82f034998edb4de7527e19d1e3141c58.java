package org.example.www.Lecture;

public class LectureProxy implements org.example.www.Lecture.Lecture_PortType {
  private String _endpoint = null;
  private org.example.www.Lecture.Lecture_PortType lecture_PortType = null;
  
  public LectureProxy() {
    _initLectureProxy();
  }
  
  public LectureProxy(String endpoint) {
    _endpoint = endpoint;
    _initLectureProxy();
  }
  
  private void _initLectureProxy() {
    try {
      lecture_PortType = (new org.example.www.Lecture.Lecture_ServiceLocator()).getLectureSOAP();
      if (lecture_PortType != null) {
        if (_endpoint != null)
          ((javax.xml.rpc.Stub)lecture_PortType)._setProperty("javax.xml.rpc.service.endpoint.address", _endpoint);
        else
          _endpoint = (String)((javax.xml.rpc.Stub)lecture_PortType)._getProperty("javax.xml.rpc.service.endpoint.address");
      }
      
    }
    catch (javax.xml.rpc.ServiceException serviceException) {}
  }
  
  public String getEndpoint() {
    return _endpoint;
  }
  
  public void setEndpoint(String endpoint) {
    _endpoint = endpoint;
    if (lecture_PortType != null)
      ((javax.xml.rpc.Stub)lecture_PortType)._setProperty("javax.xml.rpc.service.endpoint.address", _endpoint);
    
  }
  
  public org.example.www.Lecture.Lecture_PortType getLecture_PortType() {
    if (lecture_PortType == null)
      _initLectureProxy();
    return lecture_PortType;
  }
  
  public java.lang.String fnGetHelloClass(java.lang.String strSubjectCode) throws java.rmi.RemoteException{
    if (lecture_PortType == null)
      _initLectureProxy();
    return lecture_PortType.fnGetHelloClass(strSubjectCode);
  }
  
  public java.lang.String fnGetNumStudents(java.lang.String strSubjectCode) throws java.rmi.RemoteException{
    if (lecture_PortType == null)
      _initLectureProxy();
    return lecture_PortType.fnGetNumStudents(strSubjectCode);
  }
  
  public void fnGetStudInfo(java.lang.String strStudentID, javax.xml.rpc.holders.StringHolder strStudName, javax.xml.rpc.holders.StringHolder strStudGender, javax.xml.rpc.holders.StringHolder strStudGroup, org.apache.axis.holders.DateHolder studYearBirth, javax.xml.rpc.holders.FloatHolder fltStudCGPA) throws java.rmi.RemoteException{
    if (lecture_PortType == null)
      _initLectureProxy();
    lecture_PortType.fnGetStudInfo(strStudentID, strStudName, strStudGender, strStudGroup, studYearBirth, fltStudCGPA);
  }
  
  public void fnGetStudQuery(java.lang.String strLogin, javax.xml.rpc.holders.StringHolder strFirstName, javax.xml.rpc.holders.StringHolder strLastName, javax.xml.rpc.holders.StringHolder strUserLvl, javax.xml.rpc.holders.StringHolder strLastLogin) throws java.rmi.RemoteException{
    if (lecture_PortType == null)
      _initLectureProxy();
    lecture_PortType.fnGetStudQuery(strLogin, strFirstName, strLastName, strUserLvl, strLastLogin);
  }
  
  
}