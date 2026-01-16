//MenuSetting
//OperatorRole settings
//PLC Watchdog Settings
//Application Storage settings (current selected screen, etc)
//Ip address and port settings

export class AppConfig{
    //Heading texts
    static MAIN_HEADING: string = "Tittel på Current side";
    static TOP_BANNER: string = "Top Banner";
    static BOTTOM_BANNER: string = "Bottom Banner";
    static SIDE_BANNER: string = "Side Banner";
    static MAIN_PAGE: string = "Main Page";  
    static RN_PAGE: string = "Roughneck";
    static MP_PAGE: string = "Mud Pump";
    static SYS_PAGE: string= "System Overview"
    static ALM_PAGE: string= "Alarm List"
    static SPLASH_PAGE: string="OnTrack 3.0 Splash Page"
    //IP Addresses:
    
   
    //AlarmGroupName:
    static ALARM_GRP: string[] = ["TD","DW","RN","PH","MP","AWD","OPR","SYS","BLK","HST","CWS"]
        
}