import os,sys,shutil
import win32ui
import pandas as pd
import numpy as np
######打开文件#####################################
dlg = win32ui.CreateFileDialog(1)  # 1表示打开文件对话框
# dlg.SetOFNInitialDir(r"D:\\")  # 设置打开文件对话框中的初始显示目录
dlg.DoModal()
path_file_name = dlg.GetPathName()  # 获取选择的文件名称
filepath_and_filename= os.path.split(path_file_name)   #j将文件名（含扩展名）和文件路径分开
filename_and_extension = os.path.splitext(filepath_and_filename[1])  #将文件名和扩展名分开
filename = filename_and_extension[0]
filename_withpath = os.path.splitext(path_file_name)[0] #路径加文件名，不包含扩展名
print(filename_withpath)
##############将excel的n行转换成n个list格式##############并将所有列名保存到一个list里面#############
dbc_excel = pd.read_excel(path_file_name, 'MQB_KCAN',header=None)
dbc_temp = np.array(dbc_excel)
dbc_list = dbc_temp.tolist()
for i in range(len(dbc_list[1])):
    if str(dbc_list[2][i]) != "nan":
        dbc_list[1][i] = dbc_list[2][i]
##################获取各个列名对应的列号##################################
Msg_index = dbc_list[1].index("Botschaft")
Id_index = dbc_list[1].index("Identifier [dez]")
Id_len_index = dbc_list[1].index("Botschaftslänge")
Sig_index = dbc_list[1].index("Signal")
Sig_len_index = dbc_list[1].index("Signal Länge [Bits]")
Off_index = dbc_list[1].index("Offset")
Skali_index = dbc_list[1].index("Skalierung")
phy_index = dbc_list[1].index("phy Werte [dez]")
Sender_index = dbc_list[0].index("Sender - Empfänger")
Roh_index = dbc_list[1].index("Rohwert [dez]")
Einheit_index = dbc_list[1].index("Einheit")
Botyp_index = dbc_list[1].index("Botschaftstyp")
Signalsendeart_index = dbc_list[1].index("Signalsendeart")
InitWert_roh_index = dbc_list[1].index("InitWert roh [dez]")
FehlerWert_index = dbc_list[1].index("FehlerWert roh [dez]")
K15Ignition_index = dbc_list[1].index("Worst Case Verhalten bei KL15 Aus")
Besch_index = dbc_list[1].index("Beschreibung")
# print(Sender_index,len(dbc_list[1]),dbc_list[1][35])
######################写入txt开头固定的几行######################
dbcName = filename_withpath+".dbc"
txtName = filename_withpath+".txt"
f=open(dbcName, "w",encoding='utf-8',errors="replace")
f.writelines("VERSION \"HNNBNNNYNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNN/4/%%%/4/'%**4NNN///\"\n")
f.writelines("\n")
f.writelines("\n")
f.write("NS_ :\n")
f.writelines("\tNS_DESC_\n\tCM_\n\tBA_DEF_\n\tBA_\n\tVAL_\n\tCAT_DEF_\n"
               "\tCAT_\n\tFILTER\n\tBA_DEF_DEF_\n\tEV_DATA_\n\tENVVAR_DATA_\n"
               "\tSGTYPE_\n\tSGTYPE_VAL_\n\tBA_DEF_SGTYPE_\n\tBA_SGTYPE_\n"
               "\tSIG_TYPE_REF_\n\tVAL_TABLE_\n\tSIG_GROUP_\n\tSIG_VALTYPE_\n"
               "\tSIGTYPE_VALTYPE_\n\tBO_TX_BU_\n\tBA_DEF_REL_\n\tBA_REL_\n"
               "\tBA_DEF_DEF_REL_\n\tBU_SG_REL_\n\tBU_EV_REL_\n\tBU_BO_REL_\n")
f.writelines("\n")
f.writelines("BS_:\n")
f.writelines("\n")
###########获取BU部分内容，即所有的节点###################################
def get_all_nodes():
    nodes_list=[]
    for i in range(Sender_index,len(dbc_list[1])):
        if str(dbc_list[1][i]) != "nan":
            nodes_list.append(dbc_list[1][i])
        else:
            break
    return nodes_list
all_nodes ="BU_: "
print(get_all_nodes())
print(len(get_all_nodes()))
for i in range(0,len(get_all_nodes())):
    all_nodes=all_nodes+get_all_nodes()[i]+" "
###############################################
f.writelines(all_nodes)
f.writelines("\n")
f.writelines("\n")
f.close()
########################################################
###########筛选接收节点#####################
Rx_Node=[]
for i in range(len(dbc_list[1])):
    if str(dbc_list[5][i]) in ["E","E*"]:
        Rx_Node.append(dbc_list[1][i])
# print(Rx_Node)
############筛选发送节点###################
Tx_Node=[]
for i in range(len(dbc_list[1])):
    if str(dbc_list[5][i]) in ["S*","0*"]:
        Tx_Node.append(dbc_list[1][i])
print(Tx_Node)
#########################将报文信号先集中在一起####################
msg_list=[]
msg_change_number=[4]
txt=""
for i in range(4,len(dbc_list)-1):
    for n in range(len(dbc_list[1])):  ######为了计算报文名##############
        if str(dbc_list[i][n]) in ["S*", "0*"]:
            Tx_Node=dbc_list[1][n]
    if str(dbc_list[i][Sig_index]) != "void":
        if dbc_list[i][Id_index] <= 2047:
            txt = "BO_ " + str(dbc_list[i][Id_index]) + " " + str(dbc_list[i][Msg_index]) + ":" + " 8 " + str(Tx_Node)
        else:
            txt = "BO_ " + str(dbc_list[i][Id_index]+ 2147483648)+ " " + str(dbc_list[i][Msg_index]) + ":" + " 8 " + str(Tx_Node)
    if dbc_list[i][Id_index] != dbc_list[i + 1][Id_index]:
        msg_list.append(txt)                                    ###将报文全写入msglist##########
        msg_change_number.append(i+1)
msg_list.append(txt)
msg_change_number.append(len(dbc_list))
print(msg_list)
print(msg_change_number)
#########################写TXT——SG_部分##########################
f=open(dbcName, "a+")
msg = 0
k = 4
for msg in range(0,len(msg_change_number)-1):
    f.write("\n"+msg_list[msg]+"\n")
    for i in range(msg_change_number[msg], msg_change_number[msg + 1]):
        Rx_Node = []
        for n in range(len(dbc_list[1])):  ##获得接受节点###
            if str(dbc_list[i][n]) in ["E", "E*"]:  ##获得接受节点###
                Rx_Node.append(dbc_list[1][n])  ##获得接受节点###
        if Rx_Node == []:
            Rx_Node = ["Vector__XXX"]
        start_bit = 0
##################单位列值########################
        Einheit_column=[]
        if str(dbc_list[i][Einheit_index]) =="nan":
            Einheit_column.append(" \"\"  ")
        else:
            Einheit_column.append(" \""+dbc_list[i][Einheit_index]+"\"  ")
#################################################
        for j in range(k, i):
            start_bit = start_bit + dbc_list[j][Sig_len_index]  #####算起始位##############
            if (dbc_list[j][Id_index] != dbc_list[j + 1][Id_index]):
                k = j + 1
#################################################
#################################################
        if str(dbc_list[i][Botyp_index])=="BAP":
            if str(dbc_list[i][Sig_index]) != "void":  ####void信号直接不写####################
                if str(dbc_list[i][phy_index]) != "nan":  ####若范围为nan的，采用枚举方法###########
                    list_phy = dbc_list[i][phy_index].split(" .. ")  #####获得范围临界值################
                    new_context = " SG_ " + str(dbc_list[i][Sig_index]) + " : " + str(start_bit) + "|" + str(
                        dbc_list[i][Sig_len_index]) + \
                                  "@1" + "+" + " (" + str(dbc_list[i][Skali_index]) \
                                  + "," + str(dbc_list[i][Off_index]) + ") " + "[" + list_phy[0] + "|" + list_phy[
                                      -1] + "]" +str(Einheit_column[0])+",".join(Rx_Node) + '\n'
                else:
                    new_context = " SG_ " + str(dbc_list[i][Sig_index]) + " : " + str(start_bit) + "|" + str(
                        dbc_list[i][Sig_len_index]) \
                                  + "@1" + "+" + " (1.0,0.0)" + " [0.0|255]" \
                                  + str(Einheit_column[0])+ ",".join(Rx_Node) + '\n'
                f.write(new_context)
        else:
            if str(dbc_list[i][Sig_index]) != "void":  ####void信号直接不写####################
                if str(dbc_list[i][phy_index]) != "nan":  ####若范围为nan的，采用枚举方法###########
                    list_phy = dbc_list[i][phy_index].split(" .. ")  #####获得范围临界值################
                    new_context = " SG_ " + str(dbc_list[i][Sig_index]) + " : " + str(start_bit) + "|" + str(
                        dbc_list[i][Sig_len_index]) + \
                                  "@1" + "+" + " (" + str(dbc_list[i][Skali_index]) \
                                  + "," + str(dbc_list[i][Off_index]) + ") " + "[" + list_phy[0] + "|" + list_phy[
                                      -1] + "]" + str(Einheit_column[0])+ ",".join(Rx_Node) + '\n'
                else:
                    new_context = " SG_ " + str(dbc_list[i][Sig_index]) + " : " + str(start_bit) + "|" + str(
                        dbc_list[i][Sig_len_index]) \
                                  + "@1" + "+" + " (1.0,0.0)" + " [" + "0" + "|" + \
                                  str(pow(2,dbc_list[i][Sig_len_index])-1) + "]" \
                                  + str(Einheit_column[0]) + ",".join(Rx_Node) + '\n'
                f.write(new_context)
################################################################
#############筛选两个以上发送节点的信号#############################
f.write("\n"+"\n")
id_list=[]
for i in range(4,len(dbc_list)-1):
    Tx_Node1 = []
    BO_TX_BU=""
    if dbc_list[i][Id_index] <= 2047:
        id = str(dbc_list[i][Id_index])
    else:
        id = str(dbc_list[i][Id_index] + 2147483648)
    for n in range(len(dbc_list[1])):
        if str(dbc_list[i][n]) in ["S*", "0*","S"]:
            Tx_Node1.append(dbc_list[1][n])
    if (len(Tx_Node1)>=2)&(id not in id_list):
        id_list.append(id)
        BO_TX_BU = "BO_TX_BU_ " + id + " :"+ ",".join(Tx_Node1) + ";"
        f.write(BO_TX_BU+"\n")
f.write("\n"+"\n")
#########################################################
##############写入信号的comment##########################
for i in range(4,len(dbc_list)-1):
    sg_comment2txt=""
    if dbc_list[i][Id_index] <= 2047:
        id = str(dbc_list[i][Id_index])
    else:
        id = str(dbc_list[i][Id_index] + 2147483648)
##############################################################
    sg_comment=""
    sg_com_list = []
    if str(dbc_list[i][-1]) != "nan":
        sg_com_list = dbc_list[i][-1].split("\n")
        sg_com_list.append("\"")
        # print(sg_com_list)
        for j in range(0,len(sg_com_list)-1):
            if sg_com_list[j] != "Kennungsfolge:":
                sg_comment = sg_comment + sg_com_list[j]
            else:
                break
 ################替换德文中不能编码的字符############################################
        sg_comment=sg_comment.replace("\"","\'")
        sg_comment=sg_comment.replace("ä","a")
        sg_comment = sg_comment.replace("ö", "o")
        sg_comment = sg_comment.replace("Ü", "U")
        sg_comment = sg_comment.replace("ü", "u")
        sg_comment = sg_comment.replace("Ä", "A")
        sg_comment = sg_comment.replace("ß", "B")
        sg_comment = sg_comment.replace("Ö", "O")
        sg_comment = sg_comment.replace("¿", "?")
        sg_comment = sg_comment.replace("²", "^2")
        sg_comment = sg_comment.replace("„", ",")
####################################################################################
        sg_comment2txt = "CM_ SG_ " + id + " " + str(dbc_list[i][Sig_index]) + " \"" + str(sg_comment) + "\";"
        f.write(sg_comment2txt +"\n")
    if dbc_list[i][Id_index] != dbc_list[i+1][Id_index]:
        f.write("\n"+"\n")
########写入信号属性部分############################################################
Sendtype=["Cyclic","OnWrite","OnWriteWithRepetition","OnChange","OnChangeWithRepetition","IfActive","IfActiveWithRepetition","NoSigSendType"\
          ,"OnChangeAndIfActive","OnChangeAndIfActiveWithRepetition"]
f.write("\n"+"\n")
#############预定义属性#########################################
f.writelines("BA_DEF_ SG_  \"GenSigSendType\" ENUM \"Cyclic\", \"OnWrite\","
             " \"OnWriteWithRepetition\", \"OnChange\", \"OnChangeWithRepetition\", "
             "\"IfActive\", \"IfActiveWithRepetition\", \"NoSigSendType\", \"OnChangeAndIfActive\", "
             "\"OnChangeAndIfActiveWithRepetition\";"\
             "\n"\
             "BA_DEF_ SG_  \"GenSigStartValue\" INT -2147483648 2147483647;"\
             "BA_DEF_ SG_  \"Fehlerwert\" HEX 0 2147483647;"\
             "\n"
             "BA_DEF_ SG_  \"GenSigSwitchedByIgnition\" ENUM \"No\", \"Yes\";"
             "\n")
######################################################
for i in range(4,len(dbc_list)-1):
    if dbc_list[i][Id_index] <= 2047:
        id = str(dbc_list[i][Id_index])
    else:
        id = str(dbc_list[i][Id_index] + 2147483648)
    BA_Sendtype=""
    BA_Startvalue=""
    BA_Fehlerwret=""
    BA_Byignition=""
    if str(dbc_list[i][Sig_index]) != "void":
        BA_Sendtype = "BA_ \"GenSigSendType\" "+"SG_ " + id + " " + str(dbc_list[i][Sig_index])+" " + str(Sendtype.index(str(dbc_list[i][Signalsendeart_index])))+";"
        f.write(BA_Sendtype+"\n")
        if str(dbc_list[i][InitWert_roh_index])!="nan":
            BA_Startvalue = "BA_ \"GenSigStartValue\" " + "SG_ " + id + " " + str(dbc_list[i][Sig_index]) + " " + str(
                dbc_list[i][InitWert_roh_index]) + ";"
        else:
            BA_Startvalue = "BA_ \"GenSigStartValue\" " + "SG_ " + id + " " + str(dbc_list[i][Sig_index]) + " " + "0" + ";"
        f.write(BA_Startvalue + "\n")
        if str(dbc_list[i][FehlerWert_index]) != "nan":
            BA_Fehlerwret = "BA_ \"Fehlerwert\" " + "SG_ " + id + " " + str(dbc_list[i][Sig_index])+ " " + str(dbc_list[i][FehlerWert_index])+";"
        else:
            BA_Fehlerwret = "BA_ \"Fehlerwert\" " + "SG_ " + id + " " + str(dbc_list[i][Sig_index])+" 0" + ";"
        f.write(BA_Fehlerwret + "\n")
        if str(dbc_list[i][K15Ignition_index]) == "nicht bereitgestellt":
            BA_Byignition = "BA_ \"GenSigSwitchedByIgnition\" " + "SG_ " + id + " " + str(dbc_list[i][Sig_index])+ " " + "1"+";"
        else:
            BA_Byignition = "BA_ \"GenSigSwitchedByIgnition\" " + "SG_ " + id + " " + str(
                dbc_list[i][Sig_index]) + " " + "0" + ";"
        f.write(BA_Byignition + "\n")
###########写入Value Description####################################################
for i in range(4,len(dbc_list)-1):
    if dbc_list[i][Id_index] <= 2047:
        id = str(dbc_list[i][Id_index])
    else:
        id = str(dbc_list[i][Id_index] + 2147483648)
    Roh=[]
    Besch=[]
    Val_match = ""
    Val_Des = ""
    if (str(dbc_list[i][Roh_index]) != "nan")&(str(dbc_list[i][Sig_index]) != "void"):
        Roh = str(dbc_list[i][Roh_index]).split("\n")
        Besch = str(dbc_list[i][Besch_index]).split("\n")
        for j in range(0,len(Roh)):
            Val_match = Val_match + str(Roh[j]).replace("\"", "\'")+" \""+str(Besch[j]).replace("\"", "\'")+"\""
        Val_Des = "VAL_ " + id + " " + str(dbc_list[i][Sig_index]) + " " + Val_match + " ;"
        #######################################
        Val_Des = Val_Des.replace("ä", "a")
        Val_Des = Val_Des.replace("ö", "o")
        Val_Des = Val_Des.replace("Ü", "U")
        Val_Des = Val_Des.replace("ü", "u")
        Val_Des = Val_Des.replace("Ä", "A")
        Val_Des = Val_Des.replace("ß", "B")
        Val_Des = Val_Des.replace("Ö", "O")
        Val_Des = Val_Des.replace("¿", "?")
        Val_Des = Val_Des.replace("²", "^2")
        Val_Des = Val_Des.replace("„", ",")
        ######################################
        f.write(Val_Des + "\n")
f.close()
########同时添加txt格式文件##################################
shutil.copyfile(dbcName, txtName)
print("Done!")