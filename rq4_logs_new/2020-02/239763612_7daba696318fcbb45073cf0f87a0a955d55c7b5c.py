import pySaagie_connect as sc

sc.return_client_hdfs('username')

sc.return_ibis_client('username'
                      , 'password'
                      , ['dn1', 'dn2', 'dn3', 'dn4']
                      , ['nn1', 'nn2']
                      , 21050
                      , 50070)