adminUsername = "weblogicAdminUsername"
adminPassword = "weblogicAdminPassword"
adminURL = "t3://localhost:7001"
dsName = "weblogicDatasourceName"
dsJNDIName = "weblogicJndiName"
dsURL = "dataSourceUrl"
dsUsername = "dataSourceUserName"
dsPassword = "dataSourcePassword"
dsTargetName = "targetServerName"
dsDriverClass = "driverClassName"

print("*** Trying to Connect.... *****")
connect(adminUsername, adminPassword, adminURL)
print("*** Connected *****")

edit()
startEdit()

cd('/')
cmo.createJDBCSystemResource(dsName)

cd('/JDBCSystemResources/' + dsName + '/JDBCResource/' + dsName)
cmo.setName(dsName)

cd('/JDBCSystemResources/' + dsName + '/JDBCResource/' + dsName + '/JDBCDataSourceParams/' + dsName)
set('JNDINames', jarray.array([String(dsJNDIName)], String))

cd('/JDBCSystemResources/' + dsName + '/JDBCResource/' + dsName + '/JDBCDriverParams/' + dsName)
cmo.setUrl(dsURL)
cmo.setDriverName(dsDriverClass)
set('Password', dsPassword)

cd('/JDBCSystemResources/' + dsName + '/JDBCResource/' + dsName + '/JDBCConnectionPoolParams/' + dsName)
cmo.setTestTableName('SQL SELECT 1 FROM DUAL\r\n')

cd('/JDBCSystemResources/' + dsName + '/JDBCResource/' + dsName + '/JDBCDriverParams/' + dsName + '/Properties/' + dsName)
cmo.createProperty('user')

cd('/JDBCSystemResources/' + dsName + '/JDBCResource/' + dsName + '/JDBCDriverParams/' + dsName + '/Properties/' + dsName + '/Properties/user')
cmo.setValue(dsUsername)

cd('/JDBCSystemResources/' + dsName + '/JDBCResource/' + dsName + '/JDBCDataSourceParams/' + dsName)
cmo.setGlobalTransactionsProtocol('TwoPhaseCommit')

cd('/SystemResources/' + dsName)

targetList = dsTargetName.split(",")
targets = []
for targetServerName in targetList:
    targets.append(ObjectName('com.bea:Name=' + targetServerName.strip() + ',Type=Server'))

set('Targets', jarray.array(targets, ObjectName))

save()
activate()