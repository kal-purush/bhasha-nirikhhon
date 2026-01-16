from ast import parse
from curses import keyname
from http import client
import http.server
from pydoc import cli
import string
import cgi  
import base64
import json
import socketserver
from urllib.parse import urlparse, parse_qs

logged_users = {}

logged_admins = {}

containers_enable = {
    "container_12" : '1'
}

containers_fill = {
    "container_12" : [ 50, 10, 60 ]
}

data = 68

class NeuralHTTP(http.server.BaseHTTPRequestHandler):
# class NeuralHTTP(socketserver.ThreadingMixIn, http.server.HTTPServer):
    # daemon_threads = True

    def _parse_GET(self):
        getvars = parse_qs(urlparse(self.path).query)
        return getvars
    
    def do_REDIRECT(self, addr):
        self.send_response(301)
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        self.send_header('Content-type', 'text/html')
        self.send_header('Location', addr)
        self.end_headers()

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_GET(self):
        print(logged_users)
        print('------ADMINS-------')
        print(logged_admins)
        print('-------------------')
        client_ip = self.client_address[0]
        print(self.path)
        
        if (self.path.startswith('/report?')):
            self.do_HEAD()
            pass_data = self._parse_GET()
            
            with open('container_' + pass_data['id'][0] + '.json', 'r') as cont_file:
                cont_dic = json.load(cont_file)
            cont_dic['Reports'][self.date_time_string()] = {
                "Servo" : [ pass_data['s0'], pass_data['s1'], pass_data['s2'] ],
                "RGB"   : [ pass_data['r0'], pass_data['r1'], pass_data['r2'] ],
                "Level" : [ pass_data['l0'], pass_data['l1'], pass_data['l2'] ]
            }
            cont_dic['History'][self.date_time_string()] = 'New Report Added'
            with open('container_' + pass_data['id'][0] + '.json', 'w') as cont_file:
                json.dump(cont_dic, cont_file)

            return

        elif (self.path.startswith('/event/enable?')):
            self.do_HEAD()
            id = self._parse_GET()['id'][0]
            self.wfile.write(containers_enable["container_" + str(id)].encode())
            return

        elif (self.path.startswith('/event/fill?')):
            self.do_HEAD()
            fill_data = self._parse_GET()
            for iter in [ 0, 1, 2 ]:
                containers_fill['container_' + str(fill_data['id'][0])][iter] = fill_data['f' + str(iter)][0]
            
            return
            
        elif (self.path.startswith('/event/which?')):
            self.do_HEAD()
            user_id = int(self._parse_GET()['card_id'][0])

            with open('data.json', 'r') as db_file:
                db_data = json.load(db_file)
            
            for user in db_data["users"]:
                if (str(db_data['users'][user]['Card ID']) == str(user_id)):
                    form_data = db_data['users'][user]['Name'] + ' ' + str(db_data['users'][user]['Balance']) + ' ' + 'User'
                    self.wfile.write(form_data.encode())
                    return

            for tech in db_data['techs']:
                if (tech == user_id):
                    self.wfile.write('Tech'.encode())
                    return

            self.wfile.write("error".encode())
            return

        elif (self.path.startswith('/event/tech?')):
            self.do_HEAD()
            id = self._parse_GET()['id'][0]

            with open('container_' + str(id) + '.json', 'r') as cont_file:
                cont_data = json.load(cont_file)
            cont_data['History'][self.date_time_string()] = 'Maintenance'

            with open('container_' + str(id) + '.json', 'w') as cont_file:
                json.dump(cont_data, cont_file)

            return

        elif (self.path.startswith('/event/bonus_add?')):
            self.do_HEAD()
            bonus_data = self._parse_GET()
            username = ' '
            userip = ' '

            for check_ip in logged_users:
                if (str(logged_users[check_ip]['Data']['Card ID']) == str(bonus_data['card_id'][0])):
                    userip = check_ip
                    logged_users[check_ip]['Data']['Balance'] += int(bonus_data['value'][0])
                    logged_users[check_ip]['Data']['History'][self.date_time_string()] = str(bonus_data['value'][0]) + ' Bonuses Acquired'

            with open('data.json', 'r') as data_file:
                database = json.load(data_file)
                for user in database['users']:
                    if (str(database['users'][user]["Card ID"]) == str(bonus_data['card_id'][0])):
                        username = database['users'][user]['Name']

                        if (userip == ' '):
                            database['users'][user]["Balance"] += int(bonus_data['value'][0])
                            database['users'][user]['History'][self.date_time_string()] = str(bonus_data['value'][0]) + ' Bonuses Acquired'

                        else:
                            database['users'][user]['Balance'] = logged_users[userip]['Data']['Balance']
                            database['users'][user]['History'] = logged_users[userip]['Data']['History']

            if (username == ' '):
                self.wfile.write('error'.encode())
                return

            with open('data.json', 'w') as data_file:
                json.dump(database, data_file)

            with open('container_' + str(bonus_data['id'][0]) + '.json', 'r') as cont_file:
                cont_data = json.load(cont_file)
                cont_data['History'][self.date_time_string()] = username + ' obtained ' + str(bonus_data['value'][0]) + ' Bonuses'

            with open('container_' + str(bonus_data['id'][0]) + '.json', 'w') as cont_file:
                json.dump(cont_data, cont_file)

            self.wfile.write('ok'.encode())
            return

        if ((client_ip in logged_users) or (client_ip in logged_admins)):
            
            if (self.path.endswith('exit?')):
                print('exit')
                self.do_REDIRECT('/login')
                if (client_ip in logged_users):
                    logged_users.pop(client_ip)
                elif (client_ip in logged_admins):
                    logged_admins.pop(client_ip)

            elif (self.path.endswith('COMMUNAL') or self.path.endswith('INTERNET') or self.path.endswith('BUS+CARD')):
                self.do_REDIRECT('/home')
                pass_data = self._parse_GET()
                
                if (int(pass_data['sum'][0]) <= logged_users[client_ip]["Data"]['Balance']):
                    logged_users[client_ip]["Data"]['Balance'] -= int(pass_data['sum'][0])
                    if (self.path.endswith('COMMUNAL')):
                        logged_users[client_ip]["Data"]['History'][self.date_time_string()] = pass_data['sum'][0] + ' bonuses written off (Communal)'
                    elif (self.path.endswith('INTERNET')):
                        logged_users[client_ip]["Data"]['History'][self.date_time_string()] = pass_data['sum'][0] + ' bonuses written off (Internet)'
                    elif (self.path.endswith('BUS+CARD')):
                        logged_users[client_ip]['Data']['History'][self.date_time_string()] = pass_data['sum'][0] + ' bonuses written off (Bus Card)'

                    with open('data.json', 'r') as db_file:
                        database = json.load(db_file)
                    
                    database['users'][logged_users[client_ip]["Password"]]["History"] = logged_users[client_ip]["Data"]["History"]
                    database["users"][logged_users[client_ip]["Password"]]["Balance"] = logged_users[client_ip]["Data"]["Balance"]
                    
                    with open('data.json', 'w') as db_file:
                        json.dump(database, db_file)

            elif (self.path.endswith('home')):
                print('homepage')
                self.do_HEAD()
                if (client_ip in logged_users):
                    form_data =  '<html><body><meta charset="utf-8">'

                    form_data += '<style>'

                    form_data += 'body{background-color: #04AA6D; background-size: 50%}'

                    form_data += 'input[value=EXIT]{'
                    form_data += 'background-color: #DC143C; border: none; color: black; padding: 10px 32px; display: block;'
                    form_data += 'text-decoration: none; margin: 2px auto; cursor: pointer; border-radius: 5px;'
                    form_data += 'font-family: monospace; font-weight: 900}'

                    form_data += 'input[value=HISTORY], input[value=COMMUNAL], input[value=INTERNET], input[value="BUS CARD"]{'
                    form_data += 'background-color: #F5FFFA; border: none; color: black; text-align: center; display: block;'
                    form_data += 'text-decoration: none; margin: 2px auto; cursor: pointer; height: 40; width: 160; border-radius: 5px;'
                    form_data += 'font-family: monospace; font-weight: 900}</style>'

                    form_data += '<label style="background-color: white; margin: auto; padding: 8px; border: 2px solid black; width: 50%; border-radius: 5px;'
                    form_data += 'font-size: 20px; color: black; font-weight: 900; font-family: monospace; display: block; text-align: center">'
                    form_data += logged_users[client_ip]["Data"]["Name"] + '</label><br>'
                    form_data += '<label style="background-color: white; margin: auto; padding: 8px; border: 2px solid black; width: 50%; border-radius: 5px;'
                    form_data += 'font-size: 20px; color: black; font-weight: 900; font-family: monospace; display: block; text-align: center">'
                    form_data += logged_users[client_ip]["Data"]["Address"] + '</label><br>'
                    form_data += '<label style="background-color: white; margin: auto; padding: 8px; border: 2px solid black; width: 50%; border-radius: 5px;'
                    form_data += 'font-size: 20px; color: black; font-weight: 900; font-family: monospace; display: block; text-align: center">'
                    form_data += 'Balance: ' + str(logged_users[client_ip]["Data"]["Balance"]) + '</label><br>'

                    form_data += '<form method="get" action="/history">'
                    form_data += '<input type="submit" value="HISTORY">'
                    form_data += '</form>'

                    form_data += '<form method="get" action="/pay">'
                    form_data += '<label style="background-color: white; margin: auto; padding: 8px; border: 2px solid black; width: 50%; border-radius: 5px;'
                    form_data += 'font-size: 20px; color: black; font-weight: 900; font-family: monospace; display: block; text-align: center">'
                    form_data += 'Pay with bonuses</label><br>'
                    form_data += '<input name="sum" type="text" placeholder="Value"'
                    form_data += 'style="margin:auto; display: block; font-family: monospace; border-radius: 5px; padding: 6px"><br>'
                    form_data += '<input name="communal" type="submit" value="COMMUNAL">'
                    form_data += '<input name="internet" type="submit" value="INTERNET">'
                    form_data += '<input name="bus" type="submit" value="BUS CARD"><br><br>'
                    form_data += '</form>'

                    form_data += '<form method="get" action="/exit">'
                    form_data += '<input type="submit" value="EXIT">'
                    form_data += '</form>'
                                        
                    form_data += '</body></html>'
                    self.wfile.write(form_data.encode())

                elif (client_ip in logged_admins):
                    form_data =  '<html><body>'

                    form_data += '<style>'
                    
                    form_data += 'table{ font-family:monospace; border-collapse: collapse; width: 50%; margin: auto}'
                    form_data += 'td, th, tr{ border: 1px solid black; background-color: white; text-align: center; font-size: 20px;'
                    form_data += 'border-radius: 5px; border: 2px solid black; padding: 8px; font-weight: 900}'

                    form_data += 'body{background-color: #04AA6D; background-size: 50%}'

                    form_data += 'input[value=EXIT]{'
                    form_data += 'background-color: #DC143C; border: none; color: black; padding: 10px 32px; display: block;'
                    form_data += 'text-decoration: none; margin: 2px auto; cursor: pointer; border-radius: 5px;'
                    form_data += 'font-family: monospace; font-weight: 900}'                    

                    form_data += 'input[value="CONTAINER ON/OFF"], input[value="CONTAINER HISTORY"], input[value="MANAGE USERS"]{'
                    form_data += 'background-color: #F5FFFA; border: none; color: black; text-align: center; display: block;'
                    form_data += 'text-decoration: none; margin: 2px auto; cursor: pointer; border-radius: 5px;'
                    form_data += 'font-family: monospace; font-weight: 900; height: 40; width: 160}</style>'

                    form_data += '<label style="background-color: white; margin: auto; padding: 8px; border: 2px solid black; width: 50%; border-radius: 5px;'
                    form_data += 'font-size: 20px; color: black; font-weight: 900; font-family: monospace; display: block; text-align: center">'
                    form_data += logged_admins[client_ip]["Data"]["Name"] + '</label><br>'

                    form_data += '<label style="background-color: white; margin: auto; padding: 8px; border: 2px solid black; width: 50%; border-radius: 5px;'
                    form_data += 'font-size: 20px; color: black; font-weight: 900; font-family: monospace; display: block; text-align: center">'
                    form_data += 'Container Enable: '

                    if (containers_enable['container_12'] == '1'):
                        form_data += "True</label><br>"
                    else:
                        form_data += "False</label><br>"

                    form_data += '<table><tr><td colspan="3" scope="colgroup">Containers Fill</td></tr>'
                    form_data += '<tr><td>' + str(containers_fill["container_12"][0]) + ' %</td><td>' + str(containers_fill['container_12'][1]) + ' %</td><td>' + str(containers_fill['container_12'][2]) + ' %</td></tr></table><br>'

                    form_data += '<form method="get" action="/container_disable">'
                    form_data += '<input type="submit" value="CONTAINER ON/OFF"></form>'

                    form_data += '<form method="get" action="/history">'
                    form_data += '<input type="submit" value="CONTAINER HISTORY"></form>'

                    form_data += '<form method="get" action="/manage_users">'
                    form_data += '<input type="submit" value="MANAGE USERS"></form>'

                    form_data += '<form method="get" action="/exit">'
                    form_data += '<input type="submit" value="EXIT">'
                    form_data += '</form>'

                    form_data += '</body></html>'
                    self.wfile.write(form_data.encode())
            
            elif (self.path.endswith('/manage_users?')):
                self.do_HEAD()
                with open('data.json') as data_file:
                    database = json.load(data_file)
                
                form_data =  '<html><body>'

                form_data += '<style>'

                form_data += 'body{background-color: #04AA6D; background-size: 50%}'

                form_data += 'input[value="WRITE OFF"], input[value="ADD"], input[value="HOME"]{'
                form_data += 'background-color: #F5FFFA; border: none; color: black; text-align: center; display: block;'
                form_data += 'text-decoration: none; margin: 2px auto; cursor: pointer; border-radius: 5px;'
                form_data += 'font-family: monospace; font-weight: 900; height: 40; width: 160}'

                form_data += 'select[name="user_id"]{background-color:#F5FFFA; border: 2px solid black; color: black; text-align: center; display: block;'
                form_data += 'margin: 2px auto; border-radius: 5px; font-family: monospace; font-weight: 900; width: 50%}'

                form_data += '</style>'

                form_data += '<label style="background-color: white; margin: auto; padding: 8px; border: 2px solid black; width: 50%; border-radius: 5px;'
                form_data += 'font-size: 20px; color: black; font-weight: 900; font-family: monospace; display: block; text-align: center">'
                form_data += 'Users Management</label><br>'

                form_data += '<form method="get" action="/manage_users/bonuses_add">'
                form_data += '<select name="user_id" id="user_id">'
                for user in database['users']:
                    form_data += '<option value="' + str(database['users'][user]['Card ID']) + '">' + database['users'][user]['Name'] + '(' + str(database['users'][user]['Card ID']) + ') - ' + str(database['users'][user]['Balance']) + '</option>'
                form_data += '</select><br>'
                form_data += '<input name="sum" type="text" placeholder="Value"'
                form_data += 'style="margin:auto; display: block; font-family: monospace; border-radius: 5px; padding: 6px"><br>'
                form_data += '<input type="submit" name="button" value="WRITE OFF">'
                form_data += '<input type="submit" name="button" value="ADD"></form><br>'

                form_data += '<form method="get" action="/home">'
                form_data += '<input type="submit" value="HOME">'
                form_data += '</form></body></html>'

                self.wfile.write(form_data.encode())

            elif (self.path.startswith('/manage_users/bonuses_add?')):
                self.do_REDIRECT('/home')
                data = self._parse_GET()

                with open('data.json', 'r') as data_file:
                    database = json.load(data_file)

                for user in database['users']:
                    print(database['users'][user]['Card ID'])
                    print(data['user_id'][0])
                    if (str(database['users'][user]['Card ID']) == str(data['user_id'][0])):
                        if (data['button'][0] == 'WRITE OFF'):
                            database['users'][user]['Balance'] -= int(data['sum'][0])
                            database['users'][user]['History'][self.date_time_string()] = data['sum'][0] + ' Bonuses Written Off by Administrator'
                        elif (data['button'][0] == 'ADD'):
                            database['users'][user]['Balance'] += int(data['sum'][0])
                            database['users'][user]['History'][self.date_time_string()] = data['sum'][0] + ' Bonuses Added by Administrator'
                        print(database['users'][user]['Balance'])
                        with open('data.json', 'w') as w_database_file:
                            json.dump(database, w_database_file)
                        break

            elif (self.path.endswith('/container_disable?')):
                self.do_REDIRECT('/home')
                if (containers_enable['container_12'] == '1'):
                    containers_enable['container_12'] = '0'
                else:
                    containers_enable['container_12'] = '1'

            elif (self.path.endswith('/clear_reports?')):
                self.do_REDIRECT('/view_reports?')
                
                with open('container_12.json', 'r') as cont_file:
                    cont_data = json.load(cont_file)
                
                cont_data["Reports"] = { }

                with open('container_12.json', 'w') as cont_file:
                    json.dump(cont_data, cont_file)

            elif (self.path.endswith('/view_reports?')):
                self.do_HEAD()

                form_data =  '<html><body>'

                form_data += '<style>'

                form_data += 'body{background-color: #04AA6D; background-size: 50%}'

                form_data += 'table{ font-family:monospace; border-collapse: collapse; width: 75%; margin: auto}'
                form_data += 'td, th, tr{ border: 1px solid black; background-color: white; text-align: center; font-size: 20px;'
                form_data += 'border-radius: 5px; border: 2px solid black; padding: 8px; font-weight: 900}'

                form_data += 'input[value="CLEAR REPORTS"], input[value="BACK TO HISTORY"]{'
                form_data += 'background-color: #F5FFFA; border: none; color: black; text-align: center; display: block;'
                form_data += 'text-decoration: none; margin: 2px auto; cursor: pointer; border-radius: 5px;'
                form_data += 'font-family: monospace; font-weight: 900; height: 40; width: 160}'

                form_data += '</style>'

                form_data += '<label style="background-color: white; margin: auto; padding: 8px; border: 2px solid black; width: 50%; border-radius: 5px;'
                form_data += 'font-size: 20px; color: black; font-weight: 900; font-family: monospace; display: block; text-align: center">'
                form_data += 'Container Technical Reports</label><br>'

                form_data += '<form method="get" action="/clear_reports">'
                form_data += '<input type="submit" value="CLEAR REPORTS"></form>'

                form_data += '<form method="get" action="/history">'
                form_data += '<input type="submit" value="BACK TO HISTORY"></form>'
                
                with open('container_12.json', 'r') as cont_file:
                    cont_data = json.load(cont_file)

                form_data += '<label style="background-color: white; margin: auto; padding: 8px; border: 2px solid black; width: 50%; border-radius: 5px;'
                form_data += 'font-size: 20px; color: black; font-weight: 900; font-family: monospace; display: block; text-align: center">'
                form_data += 'Reports</label><br>'

                for reports in cont_data['Reports']:
                    form_data += '<table><tr><th colspan="9">' + reports + '</th></tr>'
                    form_data += '<tr><td colspan="3">Block 1</td><td colspan="3">Block 2</td><td colspan="3">Block 3</td></tr>'
                    form_data += '<tr>'
                    for iter in [1,2,3]:
                        form_data += '<td>Servo</td><td>Color</td><td>Level</td>'
                    form_data += '</tr>'
                    for iter in [1, 2, 3]:
                        form_data += '<td>' + str(cont_data['Reports'][reports]['Servo'][iter - 1][0]) + '</td>'
                        form_data += '<td>' + str(cont_data['Reports'][reports]['RGB'][iter - 1][0]) + '</td>'
                        form_data += '<td>' + str(cont_data['Reports'][reports]['Level'][iter - 1][0]) + '</td>'
                    form_data += '</tr>'
                    form_data += '</table><br>'
                
                form_data += '</body></html>'
                self.wfile.write(form_data.encode())

            elif (self.path.endswith('history?')):
                print('history')
                self.do_HEAD()
                if (client_ip in logged_users):
                    form_data =  '<html><body>'

                    form_data += '<style>'
                    form_data += 'body{background-color: #04AA6D; background-size: 50%}'

                    form_data += 'table{ font-family:monospace; border-collapse: collapse; width: 75%; margin: auto}'
                    form_data += 'td, th, tr{ border: 1px solid black; background-color: white; text-align: center; font-size: 20px;'
                    form_data += 'border-radius: 5px; border: 2px solid black; padding: 8px; font-weight: 900}'

                    form_data += 'input[value=HOME], input[value="VIEW REPORTS"], input[value="CLEAR HISTORY"]{'
                    form_data += 'background-color: #F5FFFA; border: none; color: black; text-align: center; display: block;'
                    form_data += 'text-decoration: none; margin: 2px auto; cursor: pointer; border-radius: 5px;'
                    form_data += 'font-family: monospace; font-weight: 900; height: 40; width: 160}'

                    form_data += '</style>'

                    form_data += '<label style="background-color: white; margin: auto; padding: 8px; border: 2px solid black; width: 50%; border-radius: 5px;'
                    form_data += 'font-size: 20px; color: black; font-weight: 900; font-family: monospace; display: block; text-align: center">'
                    form_data += 'User History</label><br>'
                    form_data += '<form method="get" action="/backhome">'
                    form_data += '<input type="submit" value="HOME"></form>'
                    form_data += '<form method="get" action="/history/clear">'
                    form_data += '<input type="submit" value="CLEAR HISTORY"></form>'

                    form_data += '<label style="background-color: white; margin: auto; padding: 8px; border: 2px solid black; width: 50%; border-radius: 5px;'
                    form_data += 'font-size: 20px; color: black; font-weight: 900; font-family: monospace; display: block; text-align: center">'
                    form_data += 'History</label><br>'

                    form_data += '<table><tr><th>DATE</th><th>EVENT</th></tr>'

                    for hist in logged_users[client_ip]["Data"]["History"]:
                        form_data += '<tr><td>' + hist + '</td><td>' + logged_users[client_ip]["Data"]["History"][hist] + '</td></tr>'

                    form_data += '</table>'
                
                elif (client_ip in logged_admins):
                    form_data =  '<html><body>'

                    form_data += '<style>'
                    form_data += 'body{background-color: #04AA6D; background-size: 50%}'

                    form_data += 'table{ font-family:monospace; border-collapse: collapse; width: 75%; margin: auto}'
                    form_data += 'td, th, tr{ border: 1px solid black; background-color: white; text-align: center; font-size: 20px;'
                    form_data += 'border-radius: 5px; border: 2px solid black; padding: 8px; font-weight: 900}'

                    form_data += 'input[value=HOME], input[value="VIEW REPORTS"], input[value="CLEAR HISTORY"]{'
                    form_data += 'background-color: #F5FFFA; border: none; color: black; text-align: center; display: block;'
                    form_data += 'text-decoration: none; margin: 2px auto; cursor: pointer; border-radius: 5px;'
                    form_data += 'font-family: monospace; font-weight: 900; height: 40; width: 160}'                  

                    form_data += '</style>'

                    form_data += '<label style="background-color: white; margin: auto; padding: 8px; border: 2px solid black; width: 50%; border-radius: 5px;'
                    form_data += 'font-size: 20px; color: black; font-weight: 900; font-family: monospace; display: block; text-align: center">'
                    form_data += 'Containers History</label><br>'
                    
                    form_data += '<form method="get" action="/backhome">'
                    form_data += '<input type="submit" value="HOME"></form>'
                    form_data += '<form method="get" action="/view_reports">'
                    form_data += '<input type="submit" value="VIEW REPORTS"></form>'
                    form_data += '<form method="get" action="/history/clear">'
                    form_data += '<input type="submit" value="CLEAR HISTORY"></form>'

                    form_data += '<label style="background-color: white; margin: auto; padding: 8px; border: 2px solid black; width: 50%; border-radius: 5px;'
                    form_data += 'font-size: 20px; color: black; font-weight: 900; font-family: monospace; display: block; text-align: center">'
                    form_data += 'History</label><br>'

                    with open('container_12.json', 'r') as history_file:
                        history = json.load(history_file)['History']
                    
                    form_data += '<table><tr><th>DATE</th><th>EVENT</th></tr>'

                    for hist in history:
                        form_data += '<tr><td>' + hist + '</td><td>' + history[hist] + '</td></tr>'

                    form_data += '</table>'

                form_data += '</body></html>'
                self.wfile.write(form_data.encode())

            elif (self.path.endswith('history/clear?')):
                print('clear history')
                self.do_REDIRECT('/home')
                
                if (client_ip in logged_users):
                    logged_users[client_ip]["Data"]["History"] = {}
                    with open('data.json', 'r') as database_file:
                        database = json.load(database_file)
                    database['users'][logged_users[client_ip]["Password"]]["History"] = logged_users[client_ip]["Data"]["History"]
                    with open('data.json', 'w') as database_file:
                        json.dump(database, database_file)

                elif (client_ip in logged_admins):
                    with open("container_12.json", 'r') as cont_file:
                        cont_data = json.load(cont_file)
                    cont_data["History"] = {}
                    with open('container_12.json', 'w') as cont_file:
                        json.dump(cont_data, cont_file)

            elif (self.path.endswith('backhome?')):
                self.do_REDIRECT('/home')

            else:
                self.do_REDIRECT('/home')
        
        else:
            if (self.path.startswith('/login?')):

                print('trying to log')
                pass_data = self._parse_GET()
                if ((pass_data.get('username') == None) or (pass_data.get('password') == None)):
                    self.do_REDIRECT('/login')

                res_pass = pass_data['username'][0] + ' ' + pass_data['password'][0]

                print(res_pass)

                with open("data.json", 'r') as database_file:
                    database = json.load(database_file)
                
                print(database)

                if (res_pass in database['users']):
                    logged_users.update({ client_ip : { 'Password' : res_pass, "Data" : database['users'][res_pass] }})
                    self.do_REDIRECT('/home')
                    
                elif (res_pass in database['admins']):
                    logged_admins.update({ client_ip : { 'Password' : res_pass, "Data" : database['admins'][res_pass] }})
                    self.do_REDIRECT('/home')

                else:
                    self.do_REDIRECT('/login')

            elif (self.path.endswith('/login')):
                self.do_HEAD()

                form_data =  '<html><body><meta charset="utf-8">'
                
                form_data += '<style>'
                form_data += 'body{background-color: #04AA6D; background-size: 50%}'

                form_data += 'input[value=LOGIN]{'
                form_data += 'background-color: #F5FFFA; border: none; color: black; padding: 10px 32px; display: block;'
                form_data += 'text-decoration: none; margin: 2px auto; cursor: pointer; border-radius: 5px;'
                form_data += 'font-family: monospace; font-weight: 900}'                    

                form_data += '</style>'

                form_data += '<label style="background-color: white; margin: auto; padding: 8px; border: 2px solid black; width: 50%; border-radius: 5px;'
                form_data += 'font-size: 20px; color: black; font-weight: 900; font-family: monospace; display: block; text-align: center">'    
                form_data += 'Trash Bin Server</label><br>'

                form_data += '<form method="get" action="/login">'
                form_data += '<input name="username" type="text" placeholder="User"'
                form_data += 'style="margin:auto; display: block; font-family: monospace; border-radius: 5px; padding: 6px">'
                form_data += '<input name="password" type="password" placeholder="Password"'
                form_data += 'style="margin:auto; display: block; font-family: monospace; border-radius: 5px; padding: 6px"><br>'
                form_data += '<input type="submit" value="LOGIN">'
                form_data += '</form></body></html>'

                self.wfile.write(form_data.encode())

            else:
                print('not logged')
                self.do_REDIRECT('/login')

        return

        
    def do_POST(self):
        length = int(self.headers['Content-Length'])
        post_body_bytes = self.rfile.read(length)
        post_body_text = post_body_bytes.decode()
        print(post_body_text)
        query_strings = parse_qs(post_body_text, keep_blank_values=True)

        print(query_strings)
        
        self.do_GET()

server = http.server.HTTPServer(("192.168.137.110", 8080), NeuralHTTP)
# server = NeuralHTTP(("192.168.137.110", 8080), http.server.SimpleHTTPRequestHandler)
print("RUN...")
server.serve_forever()
server.server_close()