package Table;

import scala.Array;

import java.util.ArrayList;
import java.util.List;

/**
 * Table.ROW 表
 */
public class ROW {
    public String record_type;
    public int roam_city_code;
    public int in_net_city_code;
    public int out_net_city_code;
    public String inter_network_flag;
    public String line_consult;
    public int long_distance_flag;
    public String imsi;
    public String  calling;
    public String change_flag;
    public String number_type;
    public String select_num_plan;
    public String called;
    public String service_type;
    public int service_code;
    public String double_service_type;
    public String double_service_code;
    public String req_channel;
    public String use_channel;
    public String service_hyaline;
    public String activity_code1;
    public String add_service_code1;
    public String activity_code2;
    public String add_service_code2;
    public String activity_code3;
    public String add_service_code3;
    public String activity_code4;
    public String add_service_code4;
    public String activity_code5;
    public String add_service_code5;
    public String msc;
    public String lac;
    public String cellular_flag;
    public String mobile_type_code;
    public String talk_date;
    public String talk_start_time;
    public int pay_unit;
    public int data_consult;
    public String long_distance_pay_code;
    public String other_pay_code;
    public int roam_pay;
    public int long_distance_pay;
    public int other_pay;
    public String pgy_project;
    public String sys_type_flag;
    public String speed_instr_flag;
    public String heat_pay_flag;
    public String imei;
    public ROW(){}

    public ROW(String record_type, int roam_city_code, int in_net_city_code, int out_net_city_code, String inter_network_flag, String line_consult, int long_distance_flag, String imsi, String calling, String change_flag, String number_type, String select_num_plan, String called, String service_type, int service_code, String double_service_type, String double_service_code, String req_channel, String use_channel, String service_hyaline, String activity_code1, String add_service_code1, String activity_code2, String add_service_code2, String activity_code3, String add_service_code3, String activity_code4, String add_service_code4, String activity_code5, String add_service_code5, String msc, String lac, String cellular_flag, String mobile_type_code, String talk_date, String talk_start_time, int pay_unit, int data_consult, String long_distance_pay_code, String other_pay_code, int roam_pay, int long_distance_pay, int other_pay, String pgy_project, String sys_type_flag, String speed_instr_flag, String heat_pay_flag, String imei) {
        this.record_type = record_type;
        this.roam_city_code = roam_city_code;
        this.in_net_city_code = in_net_city_code;
        this.out_net_city_code = out_net_city_code;
        this.inter_network_flag = inter_network_flag;
        this.line_consult = line_consult;
        this.long_distance_flag = long_distance_flag;
        this.imsi = imsi;
        this.calling = calling;
        this.change_flag = change_flag;
        this.number_type = number_type;
        this.select_num_plan = select_num_plan;
        this.called = called;
        this.service_type = service_type;
        this.service_code = service_code;
        this.double_service_type = double_service_type;
        this.double_service_code = double_service_code;
        this.req_channel = req_channel;
        this.use_channel = use_channel;
        this.service_hyaline = service_hyaline;
        this.activity_code1 = activity_code1;
        this.add_service_code1 = add_service_code1;
        this.activity_code2 = activity_code2;
        this.add_service_code2 = add_service_code2;
        this.activity_code3 = activity_code3;
        this.add_service_code3 = add_service_code3;
        this.activity_code4 = activity_code4;
        this.add_service_code4 = add_service_code4;
        this.activity_code5 = activity_code5;
        this.add_service_code5 = add_service_code5;
        this.msc = msc;
        this.lac = lac;
        this.cellular_flag = cellular_flag;
        this.mobile_type_code = mobile_type_code;
        this.talk_date = talk_date;
        this.talk_start_time = talk_start_time;
        this.pay_unit = pay_unit;
        this.data_consult = data_consult;
        this.long_distance_pay_code = long_distance_pay_code;
        this.other_pay_code = other_pay_code;
        this.roam_pay = roam_pay;
        this.long_distance_pay = long_distance_pay;
        this.other_pay = other_pay;
        this.pgy_project = pgy_project;
        this.sys_type_flag = sys_type_flag;
        this.speed_instr_flag = speed_instr_flag;
        this.heat_pay_flag = heat_pay_flag;
        this.imei = imei;
    }



    @Override
    public String toString() {
        return "ROW{" +
                "record_type='" + record_type + '\'' +
                ", roam_city_code=" + roam_city_code +
                ", in_net_city_code=" + in_net_city_code +
                ", out_net_city_code=" + out_net_city_code +
                ", inter_network_flag='" + inter_network_flag + '\'' +
                ", line_consult='" + line_consult + '\'' +
                ", long_distance_flag=" + long_distance_flag +
                ", imsi='" + imsi + '\'' +
                ", calling='" + calling + '\'' +
                ", change_flag='" + change_flag + '\'' +
                ", number_type='" + number_type + '\'' +
                ", select_num_plan='" + select_num_plan + '\'' +
                ", called='" + called + '\'' +
                ", service_type='" + service_type + '\'' +
                ", service_code=" + service_code +
                ", double_service_type='" + double_service_type + '\'' +
                ", double_service_code='" + double_service_code + '\'' +
                ", req_channel='" + req_channel + '\'' +
                ", use_channel='" + use_channel + '\'' +
                ", service_hyaline='" + service_hyaline + '\'' +
                ", activity_code1='" + activity_code1 + '\'' +
                ", add_service_code1='" + add_service_code1 + '\'' +
                ", activity_code2='" + activity_code2 + '\'' +
                ", add_service_code2='" + add_service_code2 + '\'' +
                ", activity_code3='" + activity_code3 + '\'' +
                ", add_service_code3='" + add_service_code3 + '\'' +
                ", activity_code4='" + activity_code4 + '\'' +
                ", add_service_code4='" + add_service_code4 + '\'' +
                ", activity_code5='" + activity_code5 + '\'' +
                ", add_service_code5='" + add_service_code5 + '\'' +
                ", msc='" + msc + '\'' +
                ", lac='" + lac + '\'' +
                ", cellular_flag='" + cellular_flag + '\'' +
                ", mobile_type_code='" + mobile_type_code + '\'' +
                ", talk_date='" + talk_date + '\'' +
                ", talk_start_time='" + talk_start_time + '\'' +
                ", pay_unit=" + pay_unit +
                ", data_consult=" + data_consult +
                ", long_distance_pay_code='" + long_distance_pay_code + '\'' +
                ", other_pay_code='" + other_pay_code + '\'' +
                ", roam_pay=" + roam_pay +
                ", long_distance_pay=" + long_distance_pay +
                ", other_pay=" + other_pay +
                ", pgy_project='" + pgy_project + '\'' +
                ", sys_type_flag='" + sys_type_flag + '\'' +
                ", speed_instr_flag='" + speed_instr_flag + '\'' +
                ", heat_pay_flag='" + heat_pay_flag + '\'' +
                ", imei='" + imei + '\'' +
                '}';
    }
}