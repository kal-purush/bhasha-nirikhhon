from PyQt4.QtGui import QTreeWidgetItem
from PyQt4         import QtGui, QtCore
from metamodel.metamodel import *

class Controller:
    def __init__(self, mainwindow):
        self.mainwindow = mainwindow
        self.numtraces = 0
        self.conttraces = 0
        self.selected_automaton = None
        self.trace_contexts = {}
        self.selected_trace = None
        self.selected_state = None
        
    def automata_new(self, q=None):
        print("Only 1 automata for now")

    def automata_open(self, q=None):
        print("No automata context load yet")

    def automata_save(self, q=None):
        print("No automata context save yet")

    def automata_duplicate(self, q=None):
        print("no automata context duplication yet")

    def automata_remove(self, q=None):
        print("no need to remove anything")

    def trace_new(self, q=None):
        if self.numtraces == 0:
            self.mainwindow.ui.automata_plaintext.setReadOnly(True)
            self.selected_automaton = ep.parse_automata(self.mainwindow.ui.automata_plaintext.toPlainText())
        self.numtraces = self.numtraces + 1
        self.conttraces = self.conttraces + 1

        if self.selected_trace != None:
            self.trace_contexts[self.selected_trace].upcoming_sequence = self.mainwindow.ui.trace_textEdit.toPlainText()
            self.trace_contexts[self.selected_trace].selstate = self.selected_state
            
        newtrace = QtGui.QTreeWidgetItem(self.mainwindow.ui.trace_list, ['Trace {}'.format(self.conttraces)])
        self.mainwindow.ui.trace_list.setCurrentItem(newtrace)
        seq = ExecutionSequence(self.selected_automaton,
                                self.selected_automaton.inistate,
                                {})
        trace_context = TraceContext(seq , self.selected_automaton.inistate,self.selected_automaton, '')
        self.selected_trace = 'Trace {}'.format(self.conttraces)
        self.trace_contexts['Trace {}'.format(self.conttraces)] = trace_context
        self.mainwindow.ui.trace_textEdit.setPlainText('')
        self.mainwindow.ui.trace_textBrowser.setPlainText('')
        self.update_analysis_states()
        self.selected_state = self.trace_contexts[self.selected_trace].selstate
        self.update_trace_browser()
        
    def trace_open(self, q=None):
        print("no trace context load yet")

    def trace_save(self, q=None):
        print("no trace context save yet")

    def trace_duplicate(self, q=None):
        print("PARA MAÑANA")

    def trace_remove(self, q=None):
        if self.numtraces == 0:
            return
        self.numtraces = self.numtraces - 1

        if self.numtraces == 0:
            self.mainwindow.ui.automata_plaintext.setReadOnly(False)
            self.mainwindow.ui.trace_textEdit.setPlainText('')
            self.mainwindow.ui.trace_textBrowser.setPlainText('')
            treew = self.mainwindow.ui.trace_list
            treew.takeTopLevelItem(treew.indexOfTopLevelItem(treew.selectedItems()[0]))
            self.mainwindow.ui.analysis_states.clear()
            return

        treew = self.mainwindow.ui.trace_list
        treew.takeTopLevelItem(treew.indexOfTopLevelItem(treew.selectedItems()[0]))
        cont = self.trace_contexts[treew.selectedItems()[0].text(0)]
        self.mainwindow.ui.trace_textEdit.setPlainText(cont.upcoming_sequence)
        self.selected_automaton = cont.automata
        self.mainwindow.ui.trace_textBrowser.setPlainText('') # cambiar                            dddddddd
        self.update_analysis_states()
        self.selected_state = self.trace_contexts[self.selected_trace].selstate
        self.update_trace_browser()
        
    def trace_fastbackward(self, q=None):
        print("PARA MAÑANA")

    def trace_backward(self, q=None):
        print("PARA MAÑANA")

    def trace_forward(self, q=None):
        upcoming = self.mainwindow.ui.trace_textEdit.toPlainText()
        self.trace_contexts[self.selected_trace].exe.step(tp.parse_test_trace(upcoming)[0])
        self.update_analysis_states()
        self.update_trace_browser()
        
    def trace_fastforward(self, q=None):
        print("PARA MAÑANA")

    def analysis_clearselected(self, q=None):
        print("PARA MAÑANA")

    def analysis_clearall(self, q=None):
        print("PARA MAÑANA")

    def analysis_outputs(self, row=None, col=None):
        print("PARA MAÑANA")

    def analysis_states(self, row=None, col=None):
        treew = self.mainwindow.ui.analysis_states
        sel = treew.selectedItems()[0]
        if sel.parent() != None:
            sel = sel.parent()
        self.selected_state = sel.text(0)
        
    def trace_list(self, row=None, col=None):
        self.trace_contexts[self.selected_trace].upcoming_sequence = self.mainwindow.ui.trace_textEdit.toPlainText()
        self.trace_contexts[self.selected_trace].selstate = self.selected_state
        
        treew = self.mainwindow.ui.trace_list
        self.selected_trace = treew.selectedItems()[0].text(0)
        cont = self.trace_contexts[treew.selectedItems()[0].text(0)]
        self.mainwindow.ui.trace_textEdit.setPlainText(cont.upcoming_sequence)
        self.selected_automaton = cont.automata
        self.update_analysis_states()
        self.selected_state = self.trace_contexts[self.selected_trace].selstate
        self.update_trace_browser()
        
    def automata_list(self, row=None, col=None):
        print("only one automata context for now")

    def update_analysis_states(self):
        exe = self.trace_contexts[self.selected_trace].exe
        self.mainwindow.ui.analysis_states.clear()
        treew = self.mainwindow.ui.analysis_states
        last = exe.v[-1]
        for it in last:
            root = QTreeWidgetItem(treew,[it, str(last[it]['eps'])])
            treew.setCurrentItem(root)
            self.selected_state = it
            for var in last[it]['varstate']:
                QTreeWidgetItem(root, [var.var, str(last[it]['varstate'][var])])

    def update_trace_browser(self):
        browser = self.mainwindow.ui.trace_textBrowser
#        self.selected_state = self.trace_contexts[self.selected_trace].selstate
        str = ''
        state = self.selected_state
        for it in self.trace_contexts[self.selected_trace].exe.v[-1:0:-1]:
            str = it[state]['action_str'] + ', ' + str
            state = it[state]['prevstate']
        browser.setPlainText(str[:-2])
"""        
def wip(q=None):
    print("work in progress...")
def processtrigger(q):
    print(q.text())
def tree(row=None, col=None):
    print("tree {} {}".format(row,col))
def treeadd(treew):
    def tree(row=None, col=None):
        root = QTreeWidgetItem(treew,['new', 'element'])
        QTreeWidgetItem(root, ['new', 'subelement'])
        treew.selectedItems()[0].setText(0,'element')
        treew.selectedItems()[0].setText(1,'clicked')
        treew.takeTopLevelItem(treew.indexOfTopLevelItem(treew.selectedItems()[0]))
#        treew.clear()
    return tree
"""