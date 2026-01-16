#!/usr/bin/python3.8
# =============================================================================
#
#   register.py
#   Version 1.0
#
#   Linux self service account creation
#
#   Valid users are checked agains LDAP
#
#   S. Traub, THU 2022
#
# =============================================================================

import sys
import os
import logging
import getpass
import subprocess
import json
import time
import socket
import secrets
import ldap

CONFIG_FILE = "config.json"
ENCODING = "utf-8"
MAXSEARCH = 1

ESC = '\033'
RED = ESC+'[0;31m'
GREEN = ESC+'[0;32m'
NO_COLOR = ESC+'[0m'

# -----------------------------------------------------------------------------

MSG = "messages"
M = MSG+"."
M_WELCOME = M+"welcome"
M_UNKNOWN_PASSWORD = M+"unknownpassword"
M_NOACCOUNT = M+"noaccount"
M_ACCOUNT_FOUND = M+"accountfound"
M_UNAUTHORIZED = M+"unauthorized"
M_ACCOUNT_CREATED = M+"accountcreated"
M_YOUR_PASSWORD = M+"yourpassword"
M_ACCOUNT_REMOVED = M+"accountremoved"
M_LOGIN_MESSAGE = M+"loginmessage"
M_SELECT_EXIT = M+"selectexit"
M_SELECT_PASSWORD_RESET = M+"passwordreset"
M_SELECT_DELETE_ACCOUNT = M+"deleteaccount"
M_PLEASE_SELECT = M+"pleaseselect"
M_BYE = M+"bye"
M_RESET_CONFIRM = M+"reset_confirm"
M_DELETE_WARNING = M+"delete_warning"
M_YESNO = M+"yesno"
M_ACCOUNT_GENERATION = M+"account_generation"
M_MAYBE_LATER = M+"maybe_later"
M_BREAK = M+"break"
M_NOTHING = M+"nothing"
M_INCORRECT = M+"incorrect"
M_FAILED = M+"failed"

msgdefaults = {
    M_WELCOME:
    "\n\nAccount Registration\n"
    "Please login with your company account.\n",

    M_UNKNOWN_PASSWORD:
    "invalid user or password, please retry",

    M_NOACCOUNT:
    "You do not have an account here yet\ncreate one?",

    M_ACCOUNT_FOUND:
    "You already have an account\n",

    M_UNAUTHORIZED:
    "Your account is valid but you are not authorized!",

    M_ACCOUNT_CREATED:
    "You can now connect here with: ssh $1@$2\n"
    "Have fun!",

    M_YOUR_PASSWORD:
    "Your generated password is: $1\n"
    "Please remeber the password, I won't tell you twice",

    M_ACCOUNT_REMOVED:
    "Your account has been removed",

    M_LOGIN_MESSAGE:
    "\nWelcome $1\n",

    M_SELECT_EXIT: "exit",
    M_SELECT_PASSWORD_RESET: "reset your password",
    M_SELECT_DELETE_ACCOUNT: "delete your account",
    M_PLEASE_SELECT: "please select",
    M_BYE: "bye",
    M_RESET_CONFIRM: "Your password has been reset",

    M_DELETE_WARNING: "Delete your account\n"
    "This will delete all your files! proceed?",

    M_YESNO: "yes/no",

    M_ACCOUNT_GENERATION: "generating your account",
    M_MAYBE_LATER: "ok, maybe later",
    M_BREAK: "session terminated",

    M_NOTHING: "Nothing done, your account is still there.",

    M_INCORRECT: "Incorrect selection!",

    M_FAILED: "Sorry, something went wrong. Please contact the admins"
}


# -----------------------------------------------------------------------------


def conf(key, default=None):
    """ return configuration from config file """

    # dot replaces subobject from config file
    k = key.split(".")

    # we handle one and two levels
    if len(k) == 1:   # in case there is no subobject
        if k[0] in config:
            return config[k[0]]     # return the value
        elif default is not None:
            return default          # or the default
        else:
            # otherwise we must terminate because the parameter is required
            print("config parameter missing: "+key)
            exit(2)

    # now handle sub objects
    # first check config and for messages "msgdefaults"
    elif len(k) == 2:
        if (k[0] in config) or (k[0] == MSG):
            if (k[0] in config) and (k[1] in config[k[0]]):
                return config[k[0]][k[1]]

            elif default is not None:
                return default

            else:
                if (k[0] == MSG) and (key in msgdefaults):
                    return msgdefaults[key]
                else:
                    # otherwise we must terminate
                    # because the parameter is required
                    print("config parameter missing: "+key)
                    exit(2)
        else:
            # otherwise we must terminate because the section is required
            print("config key missing: "+key)
            exit(2)


# -----------------------------------------------------------------------------


def get_groups(ldap_connection, member_of, group_set, recurse):
    """ get the LDAP groups the user is member of, possible recursive"""

    for m in member_of:                 # check all users groups
        grp = m.decode(ENCODING)        # decode it
        group_set.add(grp)              # add it to the group set

        # if recurse is required, we must now search the members of this group
        if recurse:
            # do the LDAP group search
            result = ldap_connection.search(
                conf("ldap.root"),
                ldap.SCOPE_SUBTREE,
                "(&(objectClass=group)(distinguishedName="+grp+"))",
                ["memberOf"])

            _, g = ldap_connection.result(result, MAXSEARCH)
            _, Attributes = g[0]        # we only expect one result

            # if the list is not empty, we must recurse
            if (Attributes is not None) and (len(Attributes)) > 0:
                mos = Attributes["memberOf"]
                get_groups(ldap_connection, mos, group_set, recurse)


# -----------------------------------------------------------------------------


def verify_password(username, password, checkgrp):
    """ Verify the users account and password using LDAP """

    time.sleep(1)      # to make brute force more difficult

    try:
        # setup the LDAP options
        Base = conf("ldap.base")
        Scope = ldap.SCOPE_SUBTREE
        Filter = conf("ldap.filter").replace("$1", username)
        Attrs = [conf("ldap.displayName")]
        Attrs.append("memberOf")

        # connect to server and search
        ldapconn = ldap.initialize(conf("ldap.host"))
        ldapconn.set_option(ldap.OPT_REFERRALS, 0)
        ldapconn.protocol_version = 3

        # check for CA certificate, if none -> use system default
        ca = conf("ldap.cacert", "")
        if ca != "":
            ldapconn.set_option(ldap.OPT_X_TLS_CACERTFILE, ca)
            ldapconn.set_option(ldap.OPT_X_TLS_NEWCTX, 0)

        ldapconn.simple_bind_s("cn=" + username + "," + Base, password)
        r = ldapconn.search(Base, Scope, Filter, Attrs)

        # get the result
        _, user = ldapconn.result(r, MAXSEARCH)
        _, Attr = user[0]
        myName = Attr[conf("ldap.displayName")][0].decode(ENCODING)

        # create an empty group list
        grplist = set()
        ing = False

        # if we mut authorize, get the users groups
        if checkgrp:
            get_groups(ldapconn,
                       Attr["memberOf"],
                       grplist,
                       conf("authorize.recurse", False))

            for ldapg in conf("authorize.member"):
                ing = ing or (ldapg in grplist)

        # the user is authorized if there is an empty "authorize" section or
        # one of the ldap groups matches
        authorized = not checkgrp or ing

    except Exception as e:
        logging.error(e)
        myName = None
        authorized = False

    return myName, authorized


# -----------------------------------------------------------------------------


def get_config_file_name():
    """ Get the name of config file """
    # check if the file exists
    if os.path.exists(CONFIG_FILE):
        return CONFIG_FILE
    else:
        print("Configfile does not exist: "+CONFIG_FILE)
        exit(1)


# -----------------------------------------------------------------------------


def load_config():
    """ load the config file """
    global config
    try:
        with open(get_config_file_name()) as config_file:  # open the file
            config = json.load(config_file)                # and parse it

    except FileNotFoundError:
        print("Config file not found")
        exit(2)


# -----------------------------------------------------------------------------


def run(cmd, cmdlog):
    """ run an os command and capture stdin and stderr """
    logging.info("execute: "+cmdlog)                          # log the command
    r = subprocess.run(cmd, shell=True, capture_output=True)  # run the process
    logging.info("stdout: "+r.stdout.decode(ENCODING))        # log stdout
    logging.info("stderr: "+r.stderr.decode(ENCODING))        # log stderr
    if r.returncode > 0:
        print(conf(M_FAILED))
        exit(2)

# -----------------------------------------------------------------------------


def set_password(user, passwd):
    """ set the users password """

    if conf("commands.Password", "copy") == "generate":
        passwd = secrets.token_urlsafe(16)
        print(conf(M_YOUR_PASSWORD).replace("$1", passwd))

    # execute all commands from config file
    for rcmd in conf("commands.setPassword"):
        cmdlog = rcmd.replace("$1", user).replace("$2", "*****")
        cmd = rcmd.replace("$1", user).replace("$2", passwd)
        run(cmd, cmdlog)


# -----------------------------------------------------------------------------


def create_user(user, gcos):
    """ create the user account """

    # read all command from config file
    for rcmd in conf("commands.createUser"):
        cmd = rcmd.replace("$1", user).replace("$2", gcos)
        run(cmd, cmd)
        print(conf(M_ACCOUNT_CREATED).replace(
            "$1", user).replace(
            "$2", socket.getfqdn()))


# -----------------------------------------------------------------------------


def delete_user(user):
    """ delete the user account """
    # execute all commands from config file
    for rcmd in conf("commands.deleteUser"):
        cmd = rcmd.replace("$1", user)
        run(cmd, cmd)
        print(conf(M_ACCOUNT_REMOVED))


# -----------------------------------------------------------------------------


if __name__ == '__main__':

    # the first argument, if any, is the config file
    if len(sys.argv) > 1:
        CONFIG_FILE = sys.argv[1]
    else:
        # the default config file is in the script direcory
        CONFIG_FILE = os.path.dirname(
            os.path.realpath(sys.argv[0]))+"/config.json"

    load_config()    # load the config

    # setup logging
    logging.basicConfig(
        format='%(asctime)s: %(levelname)s: %(message)s',
        level=conf("logging.level"),
        filename=conf("logging.file", ""))

    logging.info(
        "New connection: "+os.getenv("SSH_CONNECTION", default="UNKNOWN"))

    try:
        # print the welcome message
        print(GREEN+conf(M_WELCOME)+NO_COLOR)

        # get the username and password
        # the input is truncated for security reasons
        user = input("Username: ")[0:32]
        passwd = getpass.getpass(prompt='Password: ')[0:64]
        chkgrp = "authorize" in config

        # mow search the user in LDAP
        displayname, auth = verify_password(user, passwd, chkgrp)

        # if not found, display an errormessage
        if displayname is None:
            print(conf(M_UNKNOWN_PASSWORD))
            exit(1)

        # if the user is not authorized, inform the user
        if not auth:
            print(conf(M_UNAUTHORIZED))
            exit(1)

        # print the welcome message
        print(conf(M_LOGIN_MESSAGE).replace("$1", displayname))

        # check if the user exists by checking the home directory
        # i know, we should check passwd as well, but it's ok for now
        if os.path.exists("/home/"+user):

            # the user already exists
            print(conf(M_ACCOUNT_FOUND))

            # this is what the user can do now
            print("0: "+conf(M_SELECT_EXIT))
            print("1: "+conf(M_SELECT_PASSWORD_RESET))
            print("2: "+conf(M_SELECT_DELETE_ACCOUNT))

            # read the selection
            selection = input(conf(M_PLEASE_SELECT)+": ")[0:1]

            if selection == "0":
                print(conf(M_BYE))

            elif selection == "1":
                if conf("commands.Password", "copy") == "copy":
                    print(conf(M_RESET_CONFIRM))
                set_password(user, passwd)

            elif selection == "2":
                print(conf(M_DELETE_WARNING))
                cf = input(conf(M_YESNO)+": ")[0:1]
                if cf == conf(M_YESNO)[0]:
                    delete_user(user)
                else:
                    print(conf(M_NOTHING))

            else:
                print(conf(M_INCORRECT))

        else:
            # the user does not exist
            print(conf(M_NOACCOUNT))

            # read yes or no
            selection = input(conf(M_YESNO)+": ")[0:1]
            if selection == conf(M_YESNO)[0]:
                # if yes, generate the account and set the password
                create_user(user, displayname)
                set_password(user, passwd)
                print(conf(M_ACCOUNT_GENERATION))
            else:
                print(conf(M_MAYBE_LATER))

    except KeyboardInterrupt:
        logging.info(conf(M_BREAK))

    except Exception as ex:
        logging.info(ex.args)