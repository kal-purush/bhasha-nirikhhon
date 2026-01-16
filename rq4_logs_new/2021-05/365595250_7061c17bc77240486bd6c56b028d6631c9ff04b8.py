import os
import pyfiglet
import getpass

result = pyfiglet.figlet_format("Welcome Connections")
print(result)

password = getpass.getpass("enter the password : ")

if(password != "init"):
        print("password is incorrect...")
        exit()
        
choose = input("Where you want to run this Menu (local/remote) : ")
print(choose)

def print_msg_box(msg, indent=1, width=None, title=None):
    os.system("tput setaf 2")
    """Print message-box with optional title."""
    lines = msg.split('\n')
    space = " " * indent
    if not width:
        width = max(map(len, lines))
    box = f'╔{"═" * (width + indent * 2)}╗\n'  # upper_border
    if title:
        box += f'║{space}{title:<{width}}{space}║\n'  # title
        box += f'║{space}{"-" * len(title):<{width}}{space}║\n'  # underscore
    box += ''.join([f'║{space}{line:<{width}}{space}║\n' for line in lines])
    box += f'╚{"═" * (width + indent * 2)}╝'  # lower_border
    print(box)

if choose == "local":
        print("\t\t\tMenu Bar")
        print("\t\t----------------------------")
        msg = "Press 1 : Linux Commands\n" \
        "Press 2 : Web Server Commands\n" \
        "Press 3 : Hadoop Commands\n" \
        "Press 4 : AWS Commands\n" \
        "press 5 : Docker Commands\n" \
        "press 6 : LVM Partition Commands" 
        print_msg_box(msg=msg, indent=2, title='All Services')
        
        see = input("which command do you want to see ? : ")
        if int(see) == 1:
            while True:
                msg = "press 1 : show date\n" \
                "press 2 : show cal\n" \
                "press 3 : to check your IP\n" \
                "press 4 : to check your hard disk\n" \
                "press 5 : to create folder\n" \
                "print 6 : to create file\n" \
                "press 7 : to shutdown your system\n" \
                "press 8 : to restart your system\n" \
                "press 9 : to check your spaces in your file system\n" \
                "press 10 : to check IP active or not\n" \
                "press 11 : to install software\n" \
                "press 12 : to uninstall software" 
                print_msg_box(msg=msg, indent=2, title='Linux Commands')
                
                n = input("enter your choice : ")
                if int(n) == 1:
                    os.system("date")
                elif int(n) == 2:
                    os.system("cal")
                            
                elif int(n) == 3:
                    os.system("ifconfig enp0s3")             
                elif int(n) == 4:
                    os.system("fdisk -l")
                elif int(n) == 5:
                    fold = input("give folder name : ")
                    os.system("mkdir {}".format(fold))
                elif int(n) == 6:
                    f_name = input("give file name : ")
                    os.system("vi {}".format(f_name))
                elif int(n) == 7:
                    os.system("init 0")
                elif int(n) == 8:
                    os.system("init 6")
                elif int(n) == 9:
                    os.system("df -h")
                elif int(n) == 10:
                    ip = input("enter your IP : ")
                    os.system("ping {}".format(ip))
                elif int(n) == 11:
                    soft_name = input("enter the software name : ")
                    os.system("yum install {}".format(soft_name))
                elif int(n) == 11:
                    rm_soft = input("enter the software name : ")
                    os.system("yum remove {}".format(rm_soft))   
                    
                else:
                    print("invalid input")
                    input("please choose correct option : ")
                    
        if int(see) == 2:
            while True:
                msg = "press 1 : to install httpd server\n" \
                "press 2 : to start httpd server\n" \
                "press 3 : to restart httpd server\n" \
                "press 4 : to stop httpd server\n" \
                "press 5 : to enable httpd server\n" \
                "print 6 : to desable httpd server\n" \
                "press 7 : to start firewall\n" \
                "press 8 : to stop firewall\n" \
                "press 9 : to check web server status" 
                print_msg_box(msg=msg, indent=2, title='Web Server Commands')
                    
                t = input("enter your choice : ")
                if int(t) == 1:
                        os.system("yum install httpd")
                elif int(t) == 2:
                        os.system("systemctl start httpd")
                elif int(t) == 3:
                        os.system("systemctl restart httpd")             
                elif int(t) == 4:
                        os.system("systemctl stop httpd")
                elif int(t) == 5:
                        os.system("systemctl enable httpd")
                elif int(t) == 6:
                        os.system("systemctl desable httpd")
                elif int(t) == 7:
                        os.system("systemctl start firewalld")
                elif int(t) == 8:
                        os.system("systemctl stop firewalld")
                elif int(t) == 9:
                        os.system("systemctl status httpd")
                else:
                        print("invalid input")
                        input("please choose correct option : ")
                        
                            
        if int(see) == 3: 
            while True:
                msg = "press 1: to configure Namenode\n"
                "press 2: to configure Datanode\n"
                "press 3: for format namenode\n"
                "press 4: for start namenode\n"
                "press 5: for safemode on\n"
                "press 6: for safemode off\n"
                "press 7: for check report\n"    
                "press 8: for start datanode\n"
                "press 9: for stop datanode\n"
                "press 10: for stop nameode\n"
                "press 11: for check port connection\n" 
                print_msg_box(msg=msg, indent=2, title='Hadoop Commands')
                    
                h = input("enter your choice : ")
                if int(h) == 1:
                        f = open("/etc/hadoop/hdfs-site.xml", "w")
                        f.write("""
                        <?xml version="1.0"?>
                        <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

                        <!-- Put site-specific property overrides in this file. -->

                        <configuration>
                        <property>
                        <name>dfs.name.dir</name>
                        <value>/node1</value>.format(fn)
                        </property>
                        </configuration>
                        """)
                        f.close()

                        f = open("/etc/hadoop/core-site.xml", "w")
                        ip = input("input the ip :")
                        f.write("""<?xml version="1.0"?>
                        <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

                        <!-- Put site-specific property overrides in this file. -->

                        <configuration>
                        <property>
                        <name>fs.default.dir</name>
                        <value>hdfs://0.0.0.0</value>
                        </property>
                        </configuration>
                        """)
                        f.close()
                        print("your NameNode is configured successfully")
                        time.sleep(3)
                    
                elif int(h) == 2:
                        f = open("/etc/hadoop/hdfs-site.xml", "w")
                        f.write("""
                        <?xml version="1.0"?>
                        <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

                        <!-- Put site-specific property overrides in this file. -->

                        <configuration>
                        <property>
                        <name>dfs.data.dir</name>
                        <value>/data1</value>.format(fn)
                        </property>
                        </configuration>
                         """)
                        f.close()

                        f = open("/etc/hadoop/core-site.xml", "w")
                        ip = input("input the ip :")
                        f.write("""<?xml version="1.0"?>
                        <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

                        <!-- Put site-specific property overrides in this file. -->

                        <configuration>
                        <property>
                        <name>fs.default.dir</name>
                        <value>hdfs://192.168.43.131</value>
                        </property>
                        </configuration>
                         """)
                        f.close()
                        print("your DataNode is configured successfully")
                        time.sleep(3)
                    
                    
                elif int(h) == 3:
                            os.system("hadoop namenode -format")
                elif int(h) == 4:  
                        os.system("hadoop daemon.sh start namenode")
                elif int(h) == 5:
                        os.system("hadoop dfsadmin -safemode get")             
                elif int(h) == 6:
                        os.system("hadoop dfsadmin -safemode leave")
                elif int(h) == 7:
                        os.system("hadoop dfsadmin -report")
                elif int(h) == 8:
                        os.system("hadoop daemon.sh start datanode")
                elif int(h) == 9:
                        os.system("hadoop daemon.sh stop datanode")
                elif int(h) == 10:
                        os.system("hadoop daemon.sh stop namenode")
                elif int(h) == 11:
                        os.system("netstat -tnlp")
                else: 
                        print("invalid input")
                        input("please choose correct option : ")        
        
        if int(see) == 4:
            while True:
                msg = "press 1 : to launch instances\n"
                "press 2 : to create key-pair\n"
                "press 3 : to create security group\n"
                "press 4 : to attach volume\n"
                "press 5 : to detach volume\n"
                "print 6 : to start instances\n"
                "press 7 : to stop instances\n"
                "press 8 : to create new volume\n"
                print_msg_box(msg=msg, indent=2, title='AWS Commands')

                u = input("enter your choice : ")
                if int(u) == 1:
                        print("""\t\t\t \n
                        press 1 for launch Amazon Linux 2 AMI (HVM), SSD Volume Type
                        press 2 for launch Red Hat Enterprise Linux 8 (HVM), SSD Volume Type 
                        press 3 for launch SUSE Linux Enterprise Server 15 SP2 (HVM), SSD Volume Type 
                        press 4 for launch Ubuntu Server 20.04 LTS (HVM), SSD Volume Type
                        press 5 for launch Microsoft Windows Server 2019 Base
                        press 6 for launch Microsoft Windows Server 2019 Base with Containers 
                        press 7 for launch Microsoft Windows Server 2004 Core Base
                        press 8 for launch Microsoft Windows Server 2016 Base
                        press 9 for launch Microsoft Windows Server 2016 Base with Containers
                        press 11 for launch Ubuntu Server 18.04 LTS (HVM), SSD Volume Type
                        press 12 for launch SUSE Linux Enterprise Server 12 SP5 (HVM), SSD Volume Type 
                        press 13 for launch Microsoft Windows Server 2016 with SQL Server 2019 Standard 
                        press 14 for launch Microsoft Windows Server 2012 R2 with SQL Server 2016 Standard 
                        press 15 for launch Amazon Linux 2 with .Net Core, PowerShell, Mono, and MATE Desktop Environment
                        press 16 for launch Ubuntu Server 16.04 LTS (HVM), SSD Volume Type
                        """)

                        instances = input("which instance you want to launch : ")
                        if int(instances) == 1:
                                 os.system("aws ec2  run-instances --image-id  ami-0e306788ff2473ccb --instance-type t2.micro --count 1 --subnet-id subnet-1560697d --security-group-ids sg-0a58a842b8f3a77cc --key-name awskey")
                        elif int(instances) == 2:
                                os.system("aws ec2  run-instances --image-id ami-052c08d70def0ac62 --instance-type t2.micro --count 1 --subnet-id subnet-1560697d --security-group-ids sg-0a58a842b8f3a77cc --key-name awskey")
                        elif int(instances) == 3:
                                os.system("aws ec2  run-instances --image-id ami-0d0522ed4db1debd6 --instance-type t2.micro --count 1 --subnet-id subnet-1560697d --security-group-ids sg-0a58a842b8f3a77cc --key-name awskey")
                        elif int(instances) == 4:
                                os.system("aws ec2  run-instances --image-id ami-0cda377a1b884a1bc --instance-type t2.micro --count 1 --subnet-id subnet-1560697d --security-group-ids sg-0a58a842b8f3a77cc --key-name awskey")
                        elif int(instances) == 5:                                            
                                os.system("aws ec2  run-instances --image-id ami-0b2f6494ff0b07a0e --instance-type t2.micro --count 1 --subnet-id subnet-1560697d --security-group-ids sg-0a58a842b8f3a77cc --key-name awskey")
                        elif int(instances) == 6:
                                os.system("aws ec2  run-instances --image-id ami-0295b81b270caa9d2 --instance-type t2.micro --count 1 --subnet-id subnet-1560697d --security-group-ids sg-0a58a842b8f3a77cc --key-name awskey")
                        elif int(instances) == 7:
                                os.system("aws ec2  run-instances --image-id ami-084f975a1008b7100 --instance-type t2.micro --count 1 --subnet-id subnet-1560697d --security-group-ids sg-0a58a842b8f3a77cc --key-name awskey")
                        elif int(instances) == 8:
                                os.system("aws ec2  run-instances --image-id ami-0a0597b59c36af603 --instance-type t2.micro --count 1 --subnet-id subnet-1560697d --security-group-ids sg-0a58a842b8f3a77cc --key-name awskey")
                        elif int(instances) == 9:
                                os.system("aws ec2  run-instances --image-id ami-06c7226ccd807035b --instance-type t2.micro --count 1 --subnet-id subnet-1560697d --security-group-ids sg-0a58a842b8f3a77cc --key-name awskey")
                        elif int(instances) == 10:
                                os.system("aws ec2  run-instances --image-id ami-03f0fd1a2ba530e75 --instance-type t2.micro --count 1 --subnet-id subnet-1560697d --security-group-ids sg-0a58a842b8f3a77cc --key-name awskey")
                        elif int(instances) == 11:
                                os.system("aws ec2  run-instances --image-id ami-0d809080c3ef9dda5 --instance-type t2.micro --count 1 --subnet-id subnet-1560697d --security-group-ids sg-0a58a842b8f3a77cc --key-name awskey")
                        elif int(instances) == 12:
                                os.system("aws ec2  run-instances --image-id  ami-006ee15b4ab99e1bf --instance-type t2.micro --count 1 --subnet-id subnet-1560697d --security-group-ids sg-0a58a842b8f3a77cc --key-name awskey")
                        elif int(instances) == 13:
                                os.system("aws ec2  run-instances --image-id ami-0b5bae276bd15112d --instance-type t2.micro --count 1 --subnet-id subnet-1560697d --security-group-ids sg-0a58a842b8f3a77cc --key-name awskey")
                        elif int(instances) == 14:
                                os.system("aws ec2  run-instances --image-id ami-0ba14d654b62b98b8 --instance-type t2.micro --count 1 --subnet-id subnet-1560697d --security-group-ids sg-0a58a842b8f3a77cc --key-name awskey")
                        elif int(instances) == 15:
                                os.system("aws ec2  run-instances --image-id ami-00b494a3f139ba61f --instance-type t2.micro --count 1 --subnet-id subnet-1560697d --security-group-ids sg-0a58a842b8f3a77cc --key-name awskey")
                        elif int(instances) == 16:
                                os.system("aws ec2  run-instances --image-id ami-048bb32b1fb7c36b7 --instance-type t2.micro --count 1 --subnet-id subnet-1560697d --security-group-ids sg-0a58a842b8f3a77cc --key-name awskey")
                        else: 
                                print("invalid input")
                                input("please choose correct option : ") 
                                        
                elif int(u) == 2:
                            key = input("input your key : ")
                            name = input("enter your name : ")
                            os.system("key-name {} --query KeyMaterial > key.pem --output text > {}.pem".format(key, name))
                elif int(u) == 3:
                            group_name = input("give name : ")
                            description = input("give description : ")
                            os.system("aws ec2 create-security-group --group-name {} --description {}".format(group_name, description))             
                elif int(u) == 4:
                        instance_ID = input("enter your instance ID : ")
                        volume_ID = input("enter your volume ID : ")
                        device = input("enter your device Name ")    
                        os.system("aws ec2 attach-volume --instance-id {} --volume-id {} --device {}".format(instance_ID, volume_ID, device))
                elif int(u) == 5:
                        inst_ID = input("enter ypur instance ID : ")
                        vol_ID = input("enter your volume ID : ")
                        dev = input("enter your device Name ")    
                        os.system("aws ec2 attach-volume --instance-id {} --volume-id {} --device {}".format(inst_ID, vol_ID, dev))
                elif int(u) == 6:
                        start = input("enter your ID : ")
                        os.system("aws ec2 start-instances --instance-id {}".format(start))
                elif int(u) == 7:
                        stop = input("enter your ID : ")
                        os.system("aws ec2 stop-instances --instance-id {}".format(stop))
                elif int(u) == 8:
                        vol_size = input("enter the size of volume you want : ")
                        zone = input("enter zone : ")
                        os.system("aws ec2 create-volume —volume-type gp2 --size {} --availability-zone {}".format(vol_size, zone))
                
                else:
                        print("invalid input")
                        input("please choose correct option : ")
                            
        if int(see) == 5:
            while True:
                msg = "press 1 : to check docker is installed or not\n"
                "Press 2 : to Install docker\n"
                "Press 3 : to download docker images\n"
                "Press 4 : to see docker images available\n"
                "press 5 : to launch a docker container\n"
                "press 6 : to start a container\n"
                "press 7 : to stop a container\n"
                "press 8 : to remove a docker container\n"
                "press 9 : to remove all conatiner\n"
                "press 10 : to search for docker images\n"
                print_msg_box(msg=msg, indent=2, title='Docker Commands')
                    
                d = input("enter your choice : ")
                if int(d) == 1:                            
                        os.system("rpm -q docker-ce")
                elif int(d) == 2:
                        os.system('yum install docker-ce --nobest')
                elif int(d) == 3:        
                        image = input("enter docker image name  : ")
                        os.system("docker pull {}".format(image))
                elif int(d) == 4:
                        os.system('docker images -a')
                elif int(d) == 5:
                        i_name = input("enter docker image name : ")
                        version = input("enter the version of image : ")
                        os.system("docker run -it --name {}:{}".format(i_name, version))
                elif int(d) == 6:
                        start = input("enter container name : ")
                        os.system("docker start {}".format(start))
                elif int(d) == 7:
                        stop = input("enter container name : ")
                        os.system("docker stop {}".format(stop))
                elif int(d) == 8:
                        remove = input("enter container name : ")
                        os.system("docker rm {}".format(remove))
                elif int(d) == 9:
                        os.system("docker rm -f `docker ps -a -q`")
                elif int(d) == 10:  
                        search = input("enter docker image name you want to search: ")
                        os.system("docker search {}".format(search))
                    
                else:
                        print("invalid input")
                        input("please choose correct option : ")
                            
                            
        if int(see) == 6:
            while True:
                msg = "press q: To exit\n"
                "press 1: To see the all hard disk available in your system\n"
                "press 2: To create physical volume\n"
                "press 3: To check all physical volume available in your system\n"
                "press 4: To create volume group\n"
                "press 5: To check volume group available in your system\n"
                "press 6: To create logical volume partition\n"
                "press 7: To check logical volume partition available in your system\n"
                "press 8: To format the partition\n"
                "press 9: To create folder\n"
                "press 10: To mount the partition\n"
                "press 11: To check mounted partition\n"
                "press 12: To extend the size of logical volume partition\n"
                "press 13: To re-format the left part(remaining partition)\n"
                print_msg_box(msg=msg, indent=2, title='Partition Commands')

                choose = input("choose your requirement : ")
                os.system("tput setaf 2")
                if choose == "q":
                    exit()
                elif int(choose) == 1:
                    os.system("fdisk -l")
                    time.sleep(3)
                elif int(choose) == 2:
                    os.system("clear")
                    print("ex- sda,b,c whatever disk you have")
                    pv = input("enter the volume name : ")
                    os.system("pvcreate /dev/{}".format(pv))
                    time.sleep(3)
                elif int(choose) == 3:
                    os.system("pvdisplay")
                    time.sleep(3)

                elif int(choose) == 4:
                    print("first give new vgname then give your pv path")
                    vg = input("enter vgname :")
                    pv1 = input("enter first pv :")
                    pv2 = input("enter second pv :")
                    os.system("vgcreate {}vg /dev/{} /dev/{}".format(vg, pv1, pv2))
                    time.sleep(3)

                elif int(choose) == 5:
                    os.system("vgdisplay")
                    time.sleep(3)
                elif int(choose) == 6:
                    print("give the size in G or M then name of your lv then your vgname")
                    size = input("enter the size you want :")
                    name = input("give new lvname :")
                    gvg = input("enter vgname :")
                    os.system("lvcreate --size {} --name {} /dev/{}".format(size, name, gvg))
                    time.sleep(3)
                elif int(choose) == 7:
                    os.system("lvdisplay")
                    time.sleep(3)
                elif int(choose) == 8:
                    print("give your vgname and lvname")
                    vn = input("vgname :")
                    lvn = input("lvname :")
                    os.system("mkfs.ext4 /dev/{}/{}".format(vn, lvn))
                    time.sleep(3)
                elif int(choose) == 9:
                    f = input("give folder name :")
                    os.system("mkdir /{}".format(f))
                    print("your folder has been created successfully")
                    time.sleep(3)

                elif int(choose) == 10:
                    vgn = input("vgnme :")
                    lvnm = input("lvname :")
                    fname = input("give folder name :")
                    os.system("mount /dev/{}/{}  /{}".format(vgn, lvnm, fname))
                    time.sleep(3)

                elif int(choose) == 11:
                    os.system("df -hT")
                    time.sleep(3)
                elif int(choose) == 12:
                    size = input("give the size in M,G :")
                    vgname = input("give vgname :")
                    lvname = input("give lvname :")
                    os.system("lvextend --size +{} /dev/{}/{}".format(size, vgname, lvname))
                    time.sleep(3)
                elif int(choose) == 13:
                    vgnm = input("vgname :")
                    lvnm = input("lvname :")
                    os.system("resize2fs /dev/{}/{}".format(vgnm, lvnm))
                    time.sleep(3)
                    
                else:
                        print("invalid input")
                        input("please choose correct option : ")         
                        
        
elif choose == "no":
        exit()

else:
        print("choose valid option : ", end = '')
        ch = input() 
        