from PyQt4 import QtCore, QtGui
import os
import pickle

import core.proficiency as proficiency

import gui.player.tools as tools
from gui.player.setup_character_choice import setupCharacterChoice
from gui.player.character_generator import CharacterGenerator


class CharacterLoader(object):
    def setupUi(self, MainWindow):
        MainWindow.setObjectName(tools._fromUtf8("MainWindow"))
        MainWindow.resize(1005, 719)
        MainWindow.setWindowTitle("Character's Loader")
        self.main_window = MainWindow

        # Create main tab
        self.centralwidget = QtGui.QWidget(MainWindow)
        self.centralwidget.setObjectName(tools._fromUtf8("centralwidget"))
        MainWindow.setCentralWidget(self.centralwidget)
        
        setupCharacterChoice(self)

    def newCharacter(self):
        self.generator = CharacterGenerator()
        self.generator.setupUi()
        
    def loadCharacter(self):
        character = self.tab0_character_choice.currentItem().text()
        for c in self.list_character:
            if character == c.name:
                character = c
                break
        self.character = character


    # --------------- Character Choice ------------------------------------

    def loadListCharacters(self):
        self.list_character = []
        character_directory = os.path.join("data", "saved", "player", "")
        for character_file in os.listdir(character_directory):
            self.list_character.append(pickle.load(
                open(character_directory + character_file, 'rb')))
        self.tab0_character_choice.clear()
        for character in self.list_character:
            self.tab0_character_choice.addItem(character.name)

    def printSummary(self):
        character = self.tab0_character_choice.currentItem().text()
        for c in self.list_character:
            if character == c.name:
                character = c
                break

        self.tab0_raceLineEdit.setText(character.race.subrace_name)
        self.tab0_classLineEdit.setText(character.dnd_class.class_name)
        self.tab0_specializationLineEdit.setText(
            character.dnd_class.specialization_name)
        self.tab0_backgroundLineEdit.setText(
            character.background.background_name)
        self.tab0_experienceLevelLineEdit.setText(
            str(character.experience))
        self.tab0_str_value.setText(str(character.getStrength()))
        self.tab0_dex_value.setText(str(character.getDexterity()))
        self.tab0_con_value.setText(str(character.getConstitution()))
        self.tab0_int_value.setText(str(character.getIntelligence()))
        self.tab0_wis_value.setText(str(character.getWisdom()))
        self.tab0_cha_value.setText(str(character.getCharisma()))
        character.background.getProficiency(None)
        self.loadProficiency(character)


    def loadProficiency(self, character):
        for i in range(len(self.tab0_list_skill)):
            self.gridLayout_15.removeWidget(self.tab0_list_skill[i])
            self.tab0_list_skill[i].deleteLater()

        self.tab0_list_skill = []
        # skills
        prof = character.getProficiency()
        skills = prof.skills
        j = 0
        for i in range(len(skills)):
            if skills[i]:
                label = QtGui.QLabel(
                    tools.choiceLabel(proficiency.SkillProficiency(i).name),
                    self.tab0_skill_layout)
                label.setAlignment(QtCore.Qt.AlignCenter)
                self.gridLayout_15.addWidget(label, int(j/2), j%2)
                self.tab0_list_skill.append(label)
                j += 1

        # Saving
        saving = prof.saving
        for i in range(len(self.tab0_list_saving)):
            self.hLayout_saving.removeWidget(self.tab0_list_saving[i])
            self.tab0_list_saving[i].deleteLater()
        self.tab0_list_saving = []

        for i in range(len(saving)):
            if saving[i]:
                label = QtGui.QLabel(
                    tools.choiceLabel(proficiency.Ability(i).name),
                    self.tab0_saving_layout)
                label.setAlignment(QtCore.Qt.AlignCenter)
                self.hLayout_saving.addWidget(label)
                self.tab0_list_saving.append(label)

        # Object
        for i in range(len(self.tab0_list_object)):
            self.gridLayout_16.removeWidget(self.tab0_list_object[i])
            self.tab0_list_object[i].deleteLater()
        self.tab0_list_object = []
        weapon = prof.weapons
        j = tools.createObjectProficiencyLabel(
            self, weapon, proficiency.WeaponProficiency)
        tool = prof.tools
        j = tools.createObjectProficiencyLabel(
            self, tool, proficiency.ToolProficiency, j)
        armor = prof.armors
        j = tools.createObjectProficiencyLabel(
            self, armor, proficiency.ArmorProficiency, j)   

        # TODO add groupBox for languages
        lang = prof.languages