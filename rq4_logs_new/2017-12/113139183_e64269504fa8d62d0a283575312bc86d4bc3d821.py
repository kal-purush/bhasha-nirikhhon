from BaseCommandExec.PublishComponent import pull_code_task, compile_task, deploy_task, uninstall_service, \
    install_service, connect_server, disconnect_server

"""
 description: 这里写的封装主要考虑代码以后的扩展性
"""


def pull_code_run(pull_code_param, item_flag):
    if item_flag is True:
        pull_code_task(pull_code_param)
    else:
        pass


def compile_code_run(compile_param, item_flag):
    if item_flag is True:
        compile_task(compile_param)
    else:
        pass


def deploy_server_run(deploy_server_param):
    for deploy_server in deploy_server_param:
        server_info = {
            'IP': deploy_server['IP'],
            'UserName': deploy_server['UserName'],
            'PassWord': deploy_server['PassWord']
        }
        if deploy_server['Type'] == 'Service':                              # 如果部署的服务
            connect_server(server_info)
            uninstall_service(deploy_server)                                # 停止服务,卸载服务
            deploy_task(deploy_server)                                      # 执行部署
            install_service(deploy_server)                                  # 安装服务,启动服务
            disconnect_server(server_info)
        else:
            connect_server(server_info)
            deploy_task(deploy_server)                                      # 如果部署的站点
            disconnect_server(server_info)