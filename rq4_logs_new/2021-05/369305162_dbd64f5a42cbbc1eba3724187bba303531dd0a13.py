# -*- coding: utf-8 -*
__author__ = 'Smo'
__version__ = '0.1'

from collections import defaultdict
import idaapi
from idautils import FuncItems, CodeRefsTo
from idaapi import o_reg, o_imm, o_far, o_near, o_mem, o_displ
import os
import re
import sys
import traceback
import idc
import idautils


HAS_PYSIDE = idaapi.IDA_SDK_VERSION < 690
if HAS_PYSIDE:
    from PySide import QtGui, QtCore
    from PySide.QtGui import QTreeView, QVBoxLayout, QLineEdit, QMenu, QInputDialog, QAction, QTabWidget
else:
    from PyQt5 import QtGui, QtCore
    from PyQt5.QtWidgets import QTreeView, QVBoxLayout, QLineEdit, QMenu, QInputDialog, QAction, QTabWidget


try:
    # Python 2.
    xrange
except NameError:
    # Python 3.
    xrange = range


# enable to allow PyCharm remote debug
RDEBUG = False
# adjust this value to be a full path to a debug egg
RDEBUG_EGG = r'c:\Program Files\JetBrains\PyCharm 2017.1.4\debug-eggs\pycharm-debug.egg'
RDEBUG_HOST = 'localhost'
RDEBUG_PORT = 12321


TAGS_IGNORE_LIST = {
    'OpenProcessToken',
    'DisconnectNamedPipe'
}

IGNORE_CALL_LIST = {
    'RtlNtStatusToDosError',
    'GetLastError',
    'SetLastError'
}

TAGS = {
    'net': ['WSAStartup', 'socket', 'recv', 'recvfrom', 'send', 'sendto', 'acccept', 'bind', 'listen', 'select',
            'setsockopt', 'ioctlsocket', 'closesocket', 'WSAAccept', 'WSARecv', 'WSARecvFrom', 'WSASend', 'WSASendTo',
            'WSASocket', 'WSAConnect', 'ConnectEx', 'TransmitFile', 'HTTPOpenRequest', 'HTTPSendRequest',
            'URLDownloadToFile', 'InternetCrackUrl', 'InternetOpen', 'InternetOpen', 'InternetConnect',
            'InternetOpenUrl', 'InternetQueryOption', 'InternetSetOption', 'InternetReadFile', 'InternetWriteFile',
            'InternetGetConnectedState', 'InternetSetStatusCallback', 'DnsQuery', 'getaddrinfo', 'GetAddrInfo',
            'GetAdaptersInfo', 'GetAdaptersAddresses', 'HttpQueryInfo', 'ObtainUserAgentString', 'WNetGetProviderName',
            'GetBestInterfaceEx', 'gethostbyname', 'getsockname', 'connect', 'WinHttpOpen', 'WinHttpSetTimeouts',
            'WinHttpSendRequest', 'WinHttpConnect', 'WinHttpCrackUrl', 'WinHttpReadData', 'WinHttpOpenRequest',
            'WinHttpReceiveResponse', 'WinHttpQueryHeaders', 'HttpSendRequestW', 'HttpSendRequestA', 'HttpAddRequestHeadersW', 'HttpAddRequestHeadersA', 'HttpOpenRequestW', 'HttpOpenRequestA', 'NetServerGetInfo', 'NetApiBufferFree', 'NetWkstaGetInfo'],
    'spawn': ['CreateProcess', 'ShellExecute', 'ShellExecuteEx', 'system', 'CreateProcessInternal', 'NtCreateProcess',
              'ZwCreateProcess', 'NtCreateProcessEx', 'ZwCreateProcessEx', 'NtCreateUserProcess', 'ZwCreateUserProcess',
              'RtlCreateUserProcess', 'NtCreateSection', 'ZwCreateSection', 'NtOpenSection', 'ZwOpenSection',
              'NtAllocateVirtualMemory', 'ZwAllocateVirtualMemory', 'NtWriteVirtualMemory', 'ZwWriteVirtualMemory',
              'NtMapViewOfSection', 'ZwMapViewOfSection', 'OpenSCManager', 'CreateService', 'OpenService',
              'StartService', 'ControlService', 'ShellExecuteExA', 'ShellExecuteExW'],
    'inject': ['OpenProcess-disabled', 'ZwOpenProcess', 'NtOpenProcess', 'WriteProcessMemory', 'NtWriteVirtualMemory',
               'ZwWriteVirtualMemory', 'CreateRemoteThread', 'QueueUserAPC', 'ZwUnmapViewOfSection', 'NtUnmapViewOfSection'],
    'com': ['CoCreateInstance', 'CoInitializeSecurity', 'CoGetClassObject', 'OleConvertOLESTREAMToIStorage', 'CreateBindCtx', 'CoSetProxyBlanket', 'VariantClear'],
    'crypto': ['CryptAcquireContext', 'CryptProtectData', 'CryptUnprotectData', 'CryptProtectMemory',
               'CryptUnprotectMemory', 'CryptDecrypt', 'CryptEncrypt', 'CryptHashData', 'CryptDecodeMessage',
               'CryptDecryptMessage', 'CryptEncryptMessage', 'CryptHashMessage', 'CryptExportKey', 'CryptGenKey',
               'CryptCreateHash', 'CryptDecodeObjectEx', 'EncryptMessage', 'DecryptMessage'],
    'kbd': ['SendInput', 'VkKeyScanA', 'VkKeyScanW'],
    'file': ['_open64', 'open64', 'open', 'open64', 'fopen', 'fread', 'fclose', 'fwrite', 'flock', 'read', 'write',
             'fstat', 'lstat', 'stat', 'chmod', 'chown', 'lchown', 'link', 'symlink', 'readdir', 'readdir64'],
    'reg': ['RegOpenKeyExW', 'RegQueryValueExW', 'RegSetValueExW', 'RegCreateKeyExW', 'RegDeleteValueW', 'RegEnumKeyW', 'RegCloseKey', 'RegQueryInfoKeyW', 'RegOpenKeyExA', 'RegQueryValueExA', 'RegSetValueExA', 'RegCreateKeyExA', 'RegDeleteValueA', 'RegEnumKeyA',  'RegQueryInfoKeyA'],
    'dev': ['DeviceIoControl'],
    'wow': ['Wow64DisableWow64FsRedirection', 'Wow64RevertWow64FsRedirection']
}

STRICT_TAG_NAME_CHECKING = {'file'}

blacklist = {'@__security_check_cookie@4', '__SEH_prolog4', '__SEH_epilog4'}
replacements = [
    ('??3@YAXPAX@Z', 'alloc'),
    ('?', '')
]


def get_addr_width():
    return '16' if idaapi.cvar.inf.is_64bit() else '8'


def decode_insn(ea):
    if idaapi.IDA_SDK_VERSION >= 700 and sys.maxsize > 2**32:
        insn = idaapi.insn_t()
        if idaapi.decode_insn(insn, ea) > 0:
            return insn
    else:
        if idaapi.decode_insn(ea):
            return idaapi.cmd.copy()


def force_name(ea, new_name):
    if not ea or ea == idaapi.BADADDR:
        return
    if idaapi.IDA_SDK_VERSION >= 700:
        return idaapi.force_name(ea, new_name, idaapi.SN_NOCHECK)
    return idaapi.do_name_anyway(ea, new_name, 0)


class AutoReIDPHooks(idaapi.IDP_Hooks):
    """
    Hooks to keep view updated if some function is updated
    """
    def __init__(self, view, *args):
        super(AutoReIDPHooks, self).__init__(*args)
        self._view = view

    def __on_rename(self, ea, new_name):
        if not self._view:
            return
        items = self._view._model.findItems(('%0' + get_addr_width() + 'X') % ea, QtCore.Qt.MatchRecursive)
        if len(items) != 1:
            return

        item = items[0]
        index = self._view._model.indexFromItem(item)
        if not index.isValid():
            return

        name_index = index.sibling(index.row(), 1)
        if not name_index.isValid():
            return

        self._view._model.setData(name_index, new_name)

    def ev_rename(self, ea, new_name):
        """ callback for IDA >= 700 """
        self.__on_rename(ea, new_name)
        return super(AutoReIDPHooks, self).ev_rename(ea, new_name)

    def rename(self, ea, new_name):
        """ callback for IDA < 700 """
        self.__on_rename(ea, new_name)
        return super(AutoReIDPHooks, self).rename(ea, new_name)


class AutoREView(idaapi.PluginForm):
    ADDR_ROLE = QtCore.Qt.UserRole + 1

    OPT_FORM_PERSIST = idaapi.PluginForm.FORM_PERSIST if hasattr(idaapi.PluginForm, 'FORM_PERSIST') else idaapi.PluginForm.WOPN_PERSIST
    OPT_FORM_NO_CONTEXT = idaapi.PluginForm.FORM_NO_CONTEXT if hasattr(idaapi.PluginForm, 'FORM_NO_CONTEXT') else idaapi.PluginForm.WCLS_NO_CONTEXT

    def __init__(self, data):
        super(AutoREView, self).__init__()
        self._data = data
        self.tv = None
        self._model = None
        self._idp_hooks = None

    def Show(self):
        return idaapi.PluginForm.Show(self, 'AutoRE', options=self.OPT_FORM_PERSIST)

    def _get_parent_widget(self, form):
        if HAS_PYSIDE:
            return self.FormToPySideWidget(form)
        return self.FormToPyQtWidget(form)

    def OnCreate(self, form):
        self.parent = self._get_parent_widget(form)

        self._idp_hooks = AutoReIDPHooks(self)
        if not self._idp_hooks.hook():
            print('IDP_Hooks.hook() failed')

        self.tv = QTreeView()
        self.tv.setExpandsOnDoubleClick(False)

        root_layout = QVBoxLayout(self.parent)
        # self.le_filter = QLineEdit(self.parent)

        # root_layout.addWidget(self.le_filter)
        root_layout.addWidget(self.tv)

        self.parent.setLayout(root_layout)

        self._model = QtGui.QStandardItemModel()
        self._init_model()
        self.tv.setModel(self._model)

        self.tv.setColumnWidth(0, 200)
        self.tv.setColumnWidth(1, 300)
        self.tv.header().setStretchLastSection(True)

        self.tv.expandAll()

        self.tv.doubleClicked.connect(self.on_navigate_to_method_requested)
        # self.le_filter.textChanged.connect(self.on_filter_text_changed)
        self.tv.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
        self.tv.customContextMenuRequested.connect(self._tree_customContextMenuRequesssted)

        rename_action = QAction('Rename...', self.tv)
        rename_action.setShortcut('n')
        rename_action.triggered.connect(self._tv_rename_action_triggered)
        self.tv.addAction(rename_action)

    def _tree_customContextMenuRequesssted(self, pos):
        idx = self.tv.indexAt(pos)
        if not idx.isValid():
            return

        addr = idx.data(role=self.ADDR_ROLE)
        if not addr:
            return

        name_idx = idx.sibling(idx.row(), 1)
        old_name = name_idx.data()

        menu = QMenu()
        rename_action = menu.addAction('Rename `%s`...' % old_name)
        rename_action.setShortcut('n')
        action = menu.exec_(self.tv.mapToGlobal(pos))
        if action == rename_action:
            return self._rename_ea_requested(addr, name_idx)

    def _tv_rename_action_triggered(self):
        selected = self.tv.selectionModel().selectedIndexes()
        if not selected:
            return

        idx = selected[0]
        if not idx.isValid():
            return

        addr = idx.data(role=self.ADDR_ROLE)
        if not addr:
            return

        name_idx = idx.sibling(idx.row(), 1)
        if not name_idx.isValid():
            return

        return self._rename_ea_requested(addr, name_idx)

    def _rename_ea_requested(self, addr, name_idx):
        old_name = name_idx.data()

        if idaapi.IDA_SDK_VERSION >= 700:
            new_name = idaapi.ask_str(str(old_name), 0, 'New name:')
        else:
            new_name = idaapi.askstr(0, str(old_name), 'New name:')

        if new_name is None:
            return

        force_name(addr, new_name)
        renamed_name = idaapi.get_ea_name(addr)
        name_idx.model().setData(name_idx, renamed_name)

    def OnClose(self, form):
        if self._idp_hooks:
            self._idp_hooks.unhook()

    def _tv_init_header(self, model):
        item_header = QtGui.QStandardItem("EA")
        item_header.setToolTip("Address")
        model.setHorizontalHeaderItem(0, item_header)

        item_header = QtGui.QStandardItem("Function name")
        model.setHorizontalHeaderItem(1, item_header)

        item_header = QtGui.QStandardItem("API called")
        model.setHorizontalHeaderItem(2, item_header)

    # noinspection PyMethodMayBeStatic
    def _tv_make_tag_item(self, name):
        rv = QtGui.QStandardItem(name)

        rv.setEditable(False)
        return [rv, QtGui.QStandardItem(), QtGui.QStandardItem()]

    def _tv_make_ref_item(self, tag, ref):
        ea_item = QtGui.QStandardItem(('%0' + get_addr_width() + 'X') % ref['ea'])
        ea_item.setEditable(False)
        ea_item.setData(ref['ea'], self.ADDR_ROLE)

        name_item = QtGui.QStandardItem(ref['name'])
        name_item.setEditable(False)
        name_item.setData(ref['ea'], self.ADDR_ROLE)

        apis = ', '.join(ref['tags'][tag])
        api_name = QtGui.QStandardItem(apis)
        api_name.setEditable(False)
        api_name.setData(ref['ea'], self.ADDR_ROLE)
        api_name.setToolTip(apis)

        return [ea_item, name_item, api_name]

    def _init_model(self):
        self._model.clear()

        root_node = self._model.invisibleRootItem()
        self._tv_init_header(self._model)

        for tag, refs in self._data.items():
            item_tag_list = self._tv_make_tag_item(tag)
            item_tag = item_tag_list[0]

            root_node.appendRow(item_tag_list)

            for ref in refs:
                ref_item_list = self._tv_make_ref_item(tag, ref)

                item_tag.appendRow(ref_item_list)

    def on_navigate_to_method_requested(self, index):
        addr = index.data(role=self.ADDR_ROLE)
        if addr is not None:
            idaapi.jumpto(addr)

    # def on_filter_text_changed(self, text):
    #     print('on_text_changed: %s' % text)


class auto_re_t(idaapi.plugin_t):
    flags = idaapi.PLUGIN_UNL
    comment = ""

    help = ""
    wanted_name = "Ancestor RE"
    wanted_hotkey = "Shift+N"

    _PREFIX_NAME = 'au_re_'
    _MIN_MAX_MATH_OPS_TO_ALLOW_RENAME = 10

    _CALLEE_NODE_NAMES = {
        idaapi.PLFM_MIPS: '$ mips',
        idaapi.PLFM_ARM: '$ arm'
    }
    _DEFAULT_CALLEE_NODE_NAME = '$ vmm functions'

    _JMP_TYPES = {idaapi.NN_jmp, idaapi.NN_jmpni, idaapi.NN_jmpfi, idaapi.NN_jmpshort}

    def __init__(self):
        super(auto_re_t, self).__init__()
        self._data = None
        self.view = None

    def init(self):
        # self._cfg = None
        self.view = None
        # self._load_config()

        return idaapi.PLUGIN_OK


    def rename_parents(self,fn,user_prefix,layer):
        has_default = 0

        if layer >10:
            print("layer >10")        
            return
        for ref in idautils.CodeRefsTo(self.start_ea_of(fn), 1):
            parrent_func_p='{:x}'.format(ref)
            #if len(fn_an['math']) < self._MIN_MAX_MATH_OPS_TO_ALLOW_RENAME: jeanfixme: check the max length can be set here
            parent_name= idaapi.get_func_name(ref)
            print (parrent_func_p + ":"+ parent_name)                
        
            if not (user_prefix in parent_name):
                parent_prefix = user_prefix+'p'+str(layer)+'_'
                print ("user_prefix=" +user_prefix)
                if 'sub' in parent_name: #replace the sub_with user prefix
                    parent_new_name= parent_name.replace('sub_', parent_prefix)
                    parent_fn = idaapi.get_func(ref) 
                    force_name(self.start_ea_of(parent_fn), parent_new_name)
                    print("[parent]rename \"" + parent_name+  "\" ("+parrent_func_p+") to " +  parent_new_name)
                    self.rename_parents(parent_fn,user_prefix,layer+1)
                    has_default=1
                
                #else: jeanfixme: only rename the default one.
                    #parent_new_name= parent_prefix + parent_name
                    #print("[parent]rename \"" + parent_name+  "\" ("+parrent_func_p+") to " +  parent_new_name)
            else:
                print("user_prefix \"" + user_prefix+  "\" in ("+parrent_func_p+")  " +  parent_name)        

        return has_default

    def run(self, arg):
        try:
            self._data = dict()
            fn=idaapi.get_func(idaapi.get_screen_ea())

            if idaapi.IDA_SDK_VERSION >= 700:
                orig_name = idaapi.get_func_name(idaapi.get_screen_ea())
                addr_str='{:x}'.format(self.start_ea_of(fn))
                print ("checking function start addr: ",addr_str)
                
		#set default name
                if orig_name.lower().find(addr_str):
                    default_name = orig_name 
                else:
                    default_name = orig_name + '_'+ addr_str     #append current function address as sufix as default

                user_name = idaapi.ask_str(default_name, 0, 'New name:') 
            else:
                user_name = idaapi.askstr(0, default_name, 'New name:')  #jeanfixme: check old version support          
            
            if user_name == '':
            	return
            if orig_name == user_name:
                return
        
            #if len(fn_an['math']) < self._MIN_MAX_MATH_OPS_TO_ALLOW_RENAME: jeanfixme: check the max length can be set here
            force_name(self.start_ea_of(fn), user_name)
            
            print("rename \"" + str(orig_name)+  "\" to " +  str(user_name))       
            
            user_prefix = user_name.lower().replace(addr_str,'')
            query='Use \"'+ user_prefix+ '\" to rename the callers\' names' 
            #yesno= idaapi.askyn_c(1, query) jeanfixme: check how to interact with user 
            yesno= idaapi.ask_str("yes",0, query) #jeanfixme: check how to interact with user 
            #user the rename the parrents    
            
            if yesno == 'yes':
                #rename the parent
                print "start rename parents.."                
                self.rename_parents(fn,user_prefix,1)
            
        except:
            idaapi.msg('Ancestor RE: error: %s\n' % traceback.format_exc())

    def term(self):
        self._data = None

    @classmethod
    def get_callee_netnode(cls):
        node_name = cls._CALLEE_NODE_NAMES.get(idaapi.ph.id, cls._DEFAULT_CALLEE_NODE_NAME)
        n = idaapi.netnode(node_name)
        return n

    @classmethod
    def get_callee(cls, ea):
        n = cls.get_callee_netnode()
        v = n.altval(ea)
        v -= 1
        if v == idaapi.BADNODE:
            return
        return v
   
    @classmethod
    def start_ea_of(cls, o):
        return getattr(o, 'start_ea' if idaapi.IDA_SDK_VERSION >= 700 else 'startEA')

    @classmethod
    def end_ea_of(cls, o):
        return getattr(o, 'end_ea' if idaapi.IDA_SDK_VERSION >= 700 else 'endEA')

    @classmethod
    def get_flags_at(cls, ea):
        return getattr(idaapi, 'get_flags' if idaapi.IDA_SDK_VERSION >= 700 else 'getFlags')(ea)

    @classmethod
    def is_data(cls, flags):
        return getattr(idaapi, 'is_data' if idaapi.IDA_SDK_VERSION >= 700 else 'isData')(flags)

        

        


# noinspection PyPep8Naming
def PLUGIN_ENTRY():
    return auto_re_t()