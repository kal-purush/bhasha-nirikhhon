from unittest import TestCase
import sys
import os
import pynmrstar
import json

# Local imports
#sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))
#print (os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))
import NEFTranslator as NEFT

class TestNEFTranslator(TestCase):

    def test_read_input_file(self):
        bt = NEFT.NEFTranslator()
        read_out = bt.read_input_file('data/2mtv.nef')
        self.assertEqual(read_out[0], True)
        self.assertEqual(read_out[1], 'Entry')
        read_out = bt.read_input_file('data/saveframeonly.nef')
        self.assertEqual(read_out[0], True)
        self.assertEqual(read_out[1], 'Saveframe')
        read_out = bt.read_input_file('data/loopOnly1.nef')
        self.assertEqual(read_out[0], True)
        self.assertEqual(read_out[1], 'Loop')
        read_out = bt.read_input_file('data/nonsense.nef')
        self.assertEqual(read_out[0], False)
        self.assertEqual(read_out[1],
                         'File contains no valid saveframe or loop. Invalid file PyNMRSTAR Error:'
                         '("Invalid token found in saveframe \'internaluseyoushouldntseethis_frame\': \'A\'", 2)')

    def test_load_csv_data(self):
        bt = NEFT.NEFTranslator()
        self.assertTrue(len(bt.tagMap) > 0, "Can't read NEF-NMRSTAR_equivalence.csv or its empty")
        self.assertTrue(len(bt.NEFinfo) > 0, "Can't read NEF_mandatory.csv or its empty")

    def test_load_json_data(self):
        bt = NEFT.NEFTranslator()
        self.assertTrue(len(bt.codeDict) > 0, "Can't read codeDict.json or its empty")
        self.assertTrue(len(bt.atomDict) > 0, "Can't read atomDict.json or its empty")

    def test_get_one_letter_code(self):
        bt = NEFT.NEFTranslator()
        self.assertTrue(bt.get_one_letter_code('ALA') == 'A')
        self.assertTrue(bt.get_one_letter_code('Ala') == 'A')
        self.assertTrue(bt.get_one_letter_code('Axy') == '?')

    def test_time_stamp(self):
        bt = NEFT.NEFTranslator()
        self.assertEqual(bt.time_stamp(1556036192.7247672), '2019-04-23 16:16:32') # CDT to UTC

    def test_validate_file(self):
        bt = NEFT.NEFTranslator()
        self.assertEqual(bt.validate_file('data/xxx.xx', 'A')[0], False)  # File not found
        self.assertEqual(bt.validate_file('data/2l9r.nef', 'A')[0], True)
        self.assertEqual(bt.validate_file('data/2l9r.nef', 'S')[0], True)
        self.assertEqual(bt.validate_file('data/2l9r.nef', 'R')[0], True)
        self.assertEqual(bt.validate_file('data/2l9r.nef', 'X')[0], False)
        self.assertEqual(bt.validate_file('data/2l9r.str', 'A')[0], True)
        self.assertEqual(bt.validate_file('data/2l9r.str', 'S')[0], True)
        self.assertEqual(bt.validate_file('data/2l9r.str', 'R')[0], True)
        self.assertEqual(bt.validate_file('data/2l9r.str', 'X')[0], False)
        self.assertEqual(bt.validate_file('data/nocs.nef', 'A')[0], False)
        self.assertEqual(bt.validate_file('data/nocs.nef', 'S')[0], False)
        self.assertEqual(bt.validate_file('data/nocs.nef', 'R')[0], True)
        self.assertEqual(bt.validate_file('data/norest.nef', 'A')[0], False)
        self.assertEqual(bt.validate_file('data/norest.nef', 'S')[0], True)
        self.assertEqual(bt.validate_file('data/norest.nef', 'R')[0], False)
        self.assertEqual(bt.validate_file('data/norestcs.nef', 'A')[0], False)
        self.assertEqual(bt.validate_file('data/norestcs.nef', 'S')[0], False)
        self.assertEqual(bt.validate_file('data/norestcs.nef', 'R')[0], False)
        self.assertEqual(bt.validate_file('data/nocs.str', 'A')[0], False)
        self.assertEqual(bt.validate_file('data/nocs.str', 'S')[0], False)
        self.assertEqual(bt.validate_file('data/nocs.str', 'R')[0], True)
        self.assertEqual(bt.validate_file('data/norest.str', 'A')[0], False)
        self.assertEqual(bt.validate_file('data/norest.str', 'S')[0], True)
        self.assertEqual(bt.validate_file('data/norest.str', 'R')[0], False)
        self.assertEqual(bt.validate_file('data/norestcs.str', 'A')[0], False)
        self.assertEqual(bt.validate_file('data/norestcs.str', 'S')[0], False)
        self.assertEqual(bt.validate_file('data/norestcs.str', 'R')[0], False)
        self.assertEqual(bt.validate_file('data/nodat.str', 'R')[0], False)
        self.assertEqual(bt.validate_file('data/nodat.nef', 'R')[0], False)
        self.assertEqual(bt.validate_file('data/saveframeonly.nef', 'S')[0], True)
        self.assertEqual(bt.validate_file('data/loopOnly1.nef', 'S')[0], True)
        self.assertEqual(bt.validate_file('data/nonsense.nef', 'R')[0], False)

    def test_is_empty_loop(self):
        bt = NEFT.NEFTranslator()
        dat = pynmrstar.Entry.from_file('data/nodat.nef')
        self.assertEqual(bt.is_empty_loop(dat, '_nef_chemical_shift', 'Entry'), False)
        self.assertEqual(bt.is_empty_loop(dat, '_nef_distance_restraint', 'Entry'), True)
        dat = pynmrstar.Entry.from_file('data/nodat.str')
        self.assertEqual(bt.is_empty_loop(dat, '_Atom_chem_shift', 'Entry'), False)
        self.assertEqual(bt.is_empty_loop(dat, '_Gen_dist_constraint', 'Entry'), True)

    def test_get_data_content(self):
        bt = NEFT.NEFTranslator()
        (isValid, content, data) = bt.read_input_file('data/2mqq.nef')
        self.assertTrue(isValid)
        datacontent = bt.get_data_content(data, content)
        self.assertEqual(datacontent[0], ['nef_nmr_meta_data', 'nef_molecular_system', 'nef_chemical_shift_list',
                                          'nef_chemical_shift_list', 'nef_chemical_shift_list',
                                          'nef_distance_restraint_list',
                                          'nef_distance_restraint_list', 'nef_dihedral_restraint_list'])
        self.assertEqual(datacontent[1],
                         ['_nef_program_script', '_nef_sequence', '_nef_chemical_shift', '_nef_chemical_shift',
                          '_nef_chemical_shift', '_nef_distance_restraint', '_nef_distance_restraint',
                          '_nef_dihedral_restraint'])
        bt = NEFT.NEFTranslator()
        (isValid, content, data) = bt.read_input_file('data/2mqq.str')
        self.assertTrue(isValid)
        datacontent = bt.get_data_content(data, content)
        self.assertEqual(datacontent[0], ['entry_information', 'assembly', 'assigned_chemical_shifts',
                                          'assigned_chemical_shifts', 'assigned_chemical_shifts',
                                          'general_distance_constraints', 'general_distance_constraints',
                                          'torsion_angle_constraints'])
        self.assertEqual(datacontent[1],
                         ['_Software_applied_methods', '_Chem_comp_assembly', '_Atom_chem_shift', '_Atom_chem_shift',
                          '_Atom_chem_shift', '_Gen_dist_constraint', '_Gen_dist_constraint',
                          '_Torsion_angle_constraint'])

    def test_get_seq_from_cs_loop(self):
        bt = NEFT.NEFTranslator()
        (isValid, jsondata) = bt.get_seq_from_cs_loop('data/2mqq.nef')
        dat = json.loads(jsondata)
        self.assertTrue(isValid)
        self.assertEqual(dat['FILE'], 'NEF')
        self.assertEqual(len(dat['DATA'][0]['A']), 214)
        self.assertEqual(len(dat['DATA'][1]['B']), 5)
        self.assertEqual(len(dat['DATA'][2]['C']), 5)
        (isValid, jsondata) = bt.get_seq_from_cs_loop('data/2mqq.str')
        dat = json.loads(jsondata)
        self.assertTrue(isValid)
        self.assertEqual(dat['FILE'], 'NMR-STAR')
        self.assertEqual(len(dat['DATA'][0]['1']), 214)
        self.assertEqual(len(dat['DATA'][1]['2']), 5)
        self.assertEqual(len(dat['DATA'][2]['3']), 5)

    def test_get_nef_seq(self):
        bt = NEFT.NEFTranslator()
        dat = pynmrstar.Entry.from_file('data/2mqq.nef')
        self.assertEqual(bt.get_nef_seq(dat)[0], [{'A': ['TYR', 'GLY', 'HIS', 'ALA', 'ASP', 'SER', 'PRO', 'VAL', 'LEU',
                                                      'MET', 'VAL', 'TYR', 'GLY', 'LEU', 'ASP', 'GLN', 'SER', 'LYS',
                                                      'MET', 'ASN', 'CYS', 'ASP', 'ARG', 'VAL', 'PHE', 'ASN', 'VAL',
                                                      'PHE', 'CYS', 'LEU', 'TYR', 'GLY', 'ASN', 'VAL', 'GLU', 'LYS',
                                                      'VAL', 'LYS', 'PHE', 'MET', 'LYS', 'SER', 'LYS', 'PRO', 'GLY',
                                                      'ALA', 'ALA', 'MET', 'VAL', 'GLU', 'MET', 'ALA', 'ASP', 'GLY',
                                                      'TYR', 'ALA', 'VAL', 'ASP', 'ARG', 'ALA', 'ILE', 'THR', 'HIS',
                                                      'LEU', 'ASN', 'ASN', 'ASN', 'PHE', 'MET', 'PHE', 'GLY', 'GLN',
                                                      'LYS', 'MET', 'ASN', 'VAL', 'CYS', 'VAL', 'SER', 'LYS', 'GLN',
                                                      'PRO', 'ALA', 'ILE', 'MET', 'PRO', 'GLY', 'GLN', 'SER', 'TYR',
                                                      'GLY', 'LEU', 'GLU', 'ASP', 'GLY', 'SER', 'CYS', 'SER', 'TYR',
                                                      'LYS', 'ASP', 'PHE', 'SER', 'GLU', 'SER', 'ARG', 'ASN', 'ASN',
                                                      'ARG', 'PHE', 'SER', 'THR', 'PRO', 'GLU', 'GLN', 'ALA', 'ALA',
                                                      'LYS', 'ASN', 'ARG', 'ILE', 'GLN', 'HIS', 'PRO', 'SER', 'ASN',
                                                      'VAL', 'LEU', 'HIS', 'PHE', 'PHE', 'ASN', 'ALA', 'PRO', 'LEU',
                                                      'GLU', 'VAL', 'THR', 'GLU', 'GLU', 'ASN', 'PHE', 'PHE', 'GLU',
                                                      'ILE', 'CYS', 'ASP', 'GLU', 'LEU', 'GLY', 'VAL', 'LYS', 'ARG',
                                                      'PRO', 'THR', 'SER', 'VAL', 'LYS', 'VAL', 'PHE', 'SER', 'GLY',
                                                      'LYS', 'SER', 'GLU', 'ARG', 'SER', 'SER', 'SER', 'GLY', 'LEU',
                                                      'LEU', 'GLU', 'TRP', 'ASP', 'SER', 'LYS', 'SER', 'ASP', 'ALA',
                                                      'LEU', 'GLU', 'THR', 'LEU', 'GLY', 'PHE', 'LEU', 'ASN', 'HIS',
                                                      'TYR', 'GLN', 'MET', 'LYS', 'ASN', 'PRO', 'ASN', 'GLY', 'PRO',
                                                      'TYR', 'PRO', 'TYR', 'THR', 'LEU', 'LYS', 'LEU', 'CYS', 'PHE',
                                                      'SER', 'THR', 'ALA', 'GLN', 'HIS', 'ALA', 'SER']},
                                               {'B': ['A', 'C', 'A', 'C', 'A']},
                                               {'C': ['A', 'C', 'A', 'C', 'A']}])
        self.assertEqual(bt.get_nef_seq(dat, 'nef_sequence', 'sequence_code', 'residue_name')[0],
                         [{'A': ['TYR', 'GLY', 'PRO', 'HIS', 'ALA', 'ASP', 'SER', 'PRO', 'VAL', 'LEU', 'MET', 'VAL',
                                 'TYR', 'GLY', 'LEU', 'ASP', 'GLN', 'SER', 'LYS', 'MET', 'ASN', 'CYS', 'ASP', 'ARG',
                                 'VAL', 'PHE', 'ASN', 'VAL', 'PHE', 'CYS', 'LEU', 'TYR', 'GLY', 'ASN', 'VAL', 'GLU',
                                 'LYS', 'VAL', 'LYS', 'PHE', 'MET', 'LYS', 'SER', 'LYS', 'PRO', 'GLY', 'ALA', 'ALA',
                                 'MET', 'VAL', 'GLU', 'MET', 'ALA', 'ASP', 'GLY', 'TYR', 'ALA', 'VAL', 'ASP', 'ARG',
                                 'ALA', 'ILE', 'THR', 'HIS', 'LEU', 'ASN', 'ASN', 'ASN', 'PHE', 'MET', 'PHE', 'GLY',
                                 'GLN', 'LYS', 'MET', 'ASN', 'VAL', 'CYS', 'VAL', 'SER', 'LYS', 'GLN', 'PRO', 'ALA',
                                 'ILE', 'MET', 'PRO', 'GLY', 'GLN', 'SER', 'TYR', 'GLY', 'LEU', 'GLU', 'ASP', 'GLY',
                                 'SER', 'CYS', 'SER', 'TYR', 'LYS', 'ASP', 'PHE', 'SER', 'GLU', 'SER', 'ARG', 'ASN',
                                 'ASN', 'ARG', 'PHE', 'SER', 'THR', 'PRO', 'GLU', 'GLN', 'ALA', 'ALA', 'LYS', 'ASN',
                                 'ARG', 'ILE', 'GLN', 'HIS', 'PRO', 'SER', 'ASN', 'VAL', 'LEU', 'HIS', 'PHE', 'PHE',
                                 'ASN', 'ALA', 'PRO', 'LEU', 'GLU', 'VAL', 'THR', 'GLU', 'GLU', 'ASN', 'PHE', 'PHE',
                                 'GLU', 'ILE', 'CYS', 'ASP', 'GLU', 'LEU', 'GLY', 'VAL', 'LYS', 'ARG', 'PRO', 'THR',
                                 'SER', 'VAL', 'LYS', 'VAL', 'PHE', 'SER', 'GLY', 'LYS', 'SER', 'GLU', 'ARG', 'SER',
                                 'SER', 'SER', 'GLY', 'LEU', 'LEU', 'GLU', 'TRP', 'ASP', 'SER', 'LYS', 'SER', 'ASP',
                                 'ALA', 'LEU', 'GLU', 'THR', 'LEU', 'GLY', 'PHE', 'LEU', 'ASN', 'HIS', 'TYR', 'GLN',
                                 'MET', 'LYS', 'ASN', 'PRO', 'ASN', 'GLY', 'PRO', 'TYR', 'PRO', 'TYR', 'THR', 'LEU',
                                 'LYS', 'LEU', 'CYS', 'PHE', 'SER', 'THR', 'ALA', 'GLN', 'HIS', 'ALA', 'SER'],
                           'C': ['A', 'C', 'A', 'C', 'A'], 'B': ['A', 'C', 'A', 'C', 'A']}])
        dat = bt.read_input_file('data/saveframeonly.nef')[2]
        self.assertEqual(bt.get_nef_seq(dat)[0], [{'A': ['HIS', 'MET', 'SER', 'HIS', 'THR', 'GLN', 'VAL', 'ILE', 'GLU',
                                                      'LEU', 'GLU', 'ARG', 'LYS', 'PHE', 'SER', 'HIS', 'GLN', 'LYS',
                                                      'TYR', 'LEU', 'SER', 'ALA', 'PRO', 'GLU', 'ARG', 'ALA', 'HIS',
                                                      'LEU', 'ALA', 'LYS', 'ASN', 'LEU', 'LYS', 'LEU', 'THR', 'GLU',
                                                      'THR', 'GLN', 'VAL', 'LYS', 'ILE', 'TRP', 'PHE', 'GLN', 'ASN',
                                                      'ARG', 'ARG', 'TYR', 'LYS', 'THR', 'LYS', 'ARG', 'LYS', 'GLN',
                                                      'LEU', 'SER', 'SER', 'GLU', 'LEU', 'GLY']}])
        dat = bt.read_input_file('data/loopOnly1.nef')[2]
        self.assertEqual(bt.get_nef_seq(dat)[0], [{'A': ['HIS', 'MET', 'ASN', 'SER', 'GLN', 'ARG', 'LEU', 'ILE', 'HIS',
                                                      'ILE', 'LYS', 'THR', 'LEU', 'THR', 'THR', 'PRO', 'ASN', 'GLU',
                                                      'ASN', 'ALA', 'LEU', 'LYS', 'PHE', 'LEU', 'SER', 'THR', 'ASP',
                                                      'GLY', 'GLU', 'MET', 'LEU', 'GLN', 'THR', 'ARG', 'GLY', 'SER',
                                                      'LYS', 'SER', 'ILE', 'VAL', 'ILE', 'LYS', 'ASN', 'THR', 'ASP',
                                                      'GLU', 'ASN', 'LEU', 'ILE', 'ASN', 'HIS', 'SER', 'LYS', 'LEU',
                                                      'ALA', 'GLN', 'GLN', 'ILE', 'PHE', 'LEU', 'GLN', 'CYS', 'PRO',
                                                      'GLY', 'VAL', 'GLU', 'SER', 'LEU', 'MET', 'ILE', 'GLY', 'ASP',
                                                      'ASP', 'PHE', 'LEU', 'THR', 'ILE', 'ASN', 'LYS', 'ASP', 'ARG',
                                                      'MET', 'VAL', 'HIS', 'TRP', 'ASN', 'SER', 'ILE', 'LYS', 'PRO',
                                                      'GLU', 'ILE', 'ILE', 'ASP', 'LEU', 'LEU', 'THR', 'LYS', 'GLN',
                                                      'LEU', 'ALA', 'TYR', 'GLY', 'GLU', 'ASP', 'VAL', 'ILE', 'SER',
                                                      'LYS', 'GLU']}])
        entry = pynmrstar.Entry.from_file('data/2l9r.nef')
        # extract polymer sequence from nef_molecular_system category
        self.assertEqual(bt.get_nef_seq(entry['nef_molecular_system'], lp_category='nef_sequence')[0],
                         [{'A': ['MET', 'GLY', 'HIS', 'HIS', 'HIS', 'HIS', 'HIS', 'HIS', 'SER', 'HIS',
                                 'MET', 'SER', 'HIS', 'THR', 'GLN', 'VAL', 'ILE', 'GLU', 'LEU', 'GLU',
                                 'ARG', 'LYS', 'PHE', 'SER', 'HIS', 'GLN', 'LYS', 'TYR', 'LEU', 'SER',
                                 'ALA', 'PRO', 'GLU', 'ARG', 'ALA', 'HIS', 'LEU', 'ALA', 'LYS', 'ASN',
                                 'LEU', 'LYS', 'LEU', 'THR', 'GLU', 'THR', 'GLN', 'VAL', 'LYS', 'ILE',
                                 'TRP', 'PHE', 'GLN', 'ASN', 'ARG', 'ARG', 'TYR', 'LYS', 'THR', 'LYS',
                                 'ARG', 'LYS', 'GLN', 'LEU', 'SER', 'SER', 'GLU', 'LEU', 'GLY']}])
        self.assertEqual(bt.get_nef_seq(entry['nef_molecular_system'], lp_category='nef_sequence')[1],
                         [{'A': [i for i in range(1, 70)]}])
        # extract polymer sequence from the first cs loop in nef_chemical_shift_list category
        cs_loops = entry.get_saveframes_by_category('nef_chemical_shift_list')
        self.assertEqual(len(cs_loops), 1) # assert single cs loop
        self.assertEqual(bt.get_nef_seq(cs_loops[0], lp_category='nef_chemical_shift')[0], # select the first cs loop by input sta_data
                         [{'A': ['HIS',
                                 'MET', 'SER', 'HIS', 'THR', 'GLN', 'VAL', 'ILE', 'GLU', 'LEU', 'GLU',
                                 'ARG', 'LYS', 'PHE', 'SER', 'HIS', 'GLN', 'LYS', 'TYR', 'LEU', 'SER',
                                 'ALA', 'PRO', 'GLU', 'ARG', 'ALA', 'HIS', 'LEU', 'ALA', 'LYS', 'ASN',
                                 'LEU', 'LYS', 'LEU', 'THR', 'GLU', 'THR', 'GLN', 'VAL', 'LYS', 'ILE',
                                 'TRP', 'PHE', 'GLN', 'ASN', 'ARG', 'ARG', 'TYR', 'LYS', 'THR', 'LYS',
                                 'ARG', 'LYS', 'GLN', 'LEU', 'SER', 'SER', 'GLU', 'LEU', 'GLY']}])
        self.assertEqual(bt.get_nef_seq(cs_loops[0], lp_category='nef_chemical_shift')[1], # select the first cs loop by input sta_data
                         [{'A': [i for i in range(10, 70)]}])
        # extract polymer sequence from nef_distant_restraint_list category
        self.assertEqual(bt.get_nef_seq(entry['nef_distance_restraint_list_distance_constraint_list'], lp_category='nef_distance_restraint')[0],
                         [{'A': ['HIS',
                                 'MET', 'SER', 'HIS', 'THR', 'GLN', 'VAL', 'ILE', 'GLU', 'LEU', 'GLU',
                                 'ARG', 'LYS', 'PHE', 'SER', 'HIS', 'GLN', 'LYS', 'TYR', 'LEU', 'SER',
                                 'ALA', 'PRO', 'GLU', 'ARG', 'ALA', 'HIS', 'LEU', 'ALA', 'LYS', 'ASN',
                                 'LEU', 'LYS', 'LEU', 'THR', 'GLU', 'THR', 'GLN', 'VAL', 'LYS', 'ILE',
                                 'TRP', 'PHE', 'GLN', 'ASN', 'ARG', 'ARG', 'TYR', 'LYS', 'THR', 'LYS',
                                 'ARG', 'LYS', 'GLN', 'LEU', 'SER', 'SER', 'GLU', 'LEU', 'GLY']}])
        self.assertEqual(bt.get_nef_seq(entry['nef_distance_restraint_list_distance_constraint_list'], lp_category='nef_distance_restraint')[1],
                         [{'A': [i for i in range(10, 70)]}])

    def test_get_nmrstar_seq(self):
        bt = NEFT.NEFTranslator()
        dat = pynmrstar.Entry.from_file('data/2mqq.str')
        self.assertEqual(bt.get_nmrstar_seq(dat)[0],
                         [{'1': ['TYR', 'GLY', 'HIS', 'ALA', 'ASP', 'SER', 'PRO', 'VAL', 'LEU', 'MET', 'VAL', 'TYR',
                                 'GLY', 'LEU', 'ASP', 'GLN', 'SER', 'LYS', 'MET', 'ASN', 'CYS', 'ASP', 'ARG', 'VAL',
                                 'PHE', 'ASN', 'VAL', 'PHE', 'CYS', 'LEU', 'TYR', 'GLY', 'ASN', 'VAL', 'GLU', 'LYS',
                                 'VAL', 'LYS', 'PHE', 'MET', 'LYS', 'SER', 'LYS', 'PRO', 'GLY', 'ALA', 'ALA', 'MET',
                                 'VAL', 'GLU', 'MET', 'ALA', 'ASP', 'GLY', 'TYR', 'ALA', 'VAL', 'ASP', 'ARG', 'ALA',
                                 'ILE', 'THR', 'HIS', 'LEU', 'ASN', 'ASN', 'ASN', 'PHE', 'MET', 'PHE', 'GLY', 'GLN',
                                 'LYS', 'MET', 'ASN', 'VAL', 'CYS', 'VAL', 'SER', 'LYS', 'GLN', 'PRO', 'ALA', 'ILE',
                                 'MET', 'PRO', 'GLY', 'GLN', 'SER', 'TYR', 'GLY', 'LEU', 'GLU', 'ASP', 'GLY', 'SER',
                                 'CYS', 'SER', 'TYR', 'LYS', 'ASP', 'PHE', 'SER', 'GLU', 'SER', 'ARG', 'ASN', 'ASN',
                                 'ARG', 'PHE', 'SER', 'THR', 'PRO', 'GLU', 'GLN', 'ALA', 'ALA', 'LYS', 'ASN', 'ARG',
                                 'ILE', 'GLN', 'HIS', 'PRO', 'SER', 'ASN', 'VAL', 'LEU', 'HIS', 'PHE', 'PHE', 'ASN',
                                 'ALA', 'PRO', 'LEU', 'GLU', 'VAL', 'THR', 'GLU', 'GLU', 'ASN', 'PHE', 'PHE', 'GLU',
                                 'ILE', 'CYS', 'ASP', 'GLU', 'LEU', 'GLY', 'VAL', 'LYS', 'ARG', 'PRO', 'THR', 'SER',
                                 'VAL', 'LYS', 'VAL', 'PHE', 'SER', 'GLY', 'LYS', 'SER', 'GLU', 'ARG', 'SER', 'SER',
                                 'SER', 'GLY', 'LEU', 'LEU', 'GLU', 'TRP', 'ASP', 'SER', 'LYS', 'SER', 'ASP', 'ALA',
                                 'LEU', 'GLU', 'THR', 'LEU', 'GLY', 'PHE', 'LEU', 'ASN', 'HIS', 'TYR', 'GLN', 'MET',
                                 'LYS', 'ASN', 'PRO', 'ASN', 'GLY', 'PRO', 'TYR', 'PRO', 'TYR', 'THR', 'LEU', 'LYS',
                                 'LEU', 'CYS', 'PHE', 'SER', 'THR', 'ALA', 'GLN', 'HIS', 'ALA', 'SER']},
                          {'2': ['A', 'C', 'A', 'C', 'A']}, {'3': ['A', 'C', 'A', 'C', 'A']}])
        self.assertEqual(bt.get_nmrstar_seq(dat, 'Chem_comp_assembly', 'Comp_index_ID', 'Comp_ID')[0],
                         [{'1': ['TYR', 'GLY', 'PRO', 'HIS', 'ALA', 'ASP', 'SER', 'PRO', 'VAL', 'LEU', 'MET', 'VAL',
                                 'TYR',
                                 'GLY', 'LEU', 'ASP', 'GLN', 'SER', 'LYS', 'MET', 'ASN', 'CYS', 'ASP', 'ARG', 'VAL',
                                 'PHE', 'ASN', 'VAL', 'PHE', 'CYS', 'LEU', 'TYR', 'GLY', 'ASN', 'VAL', 'GLU', 'LYS',
                                 'VAL', 'LYS', 'PHE', 'MET', 'LYS', 'SER', 'LYS', 'PRO', 'GLY', 'ALA', 'ALA', 'MET',
                                 'VAL', 'GLU', 'MET', 'ALA', 'ASP', 'GLY', 'TYR', 'ALA', 'VAL', 'ASP', 'ARG', 'ALA',
                                 'ILE', 'THR', 'HIS', 'LEU', 'ASN', 'ASN', 'ASN', 'PHE', 'MET', 'PHE', 'GLY', 'GLN',
                                 'LYS', 'MET', 'ASN', 'VAL', 'CYS', 'VAL', 'SER', 'LYS', 'GLN', 'PRO', 'ALA', 'ILE',
                                 'MET', 'PRO', 'GLY', 'GLN', 'SER', 'TYR', 'GLY', 'LEU', 'GLU', 'ASP', 'GLY', 'SER',
                                 'CYS', 'SER', 'TYR', 'LYS', 'ASP', 'PHE', 'SER', 'GLU', 'SER', 'ARG', 'ASN', 'ASN',
                                 'ARG', 'PHE', 'SER', 'THR', 'PRO', 'GLU', 'GLN', 'ALA', 'ALA', 'LYS', 'ASN', 'ARG',
                                 'ILE', 'GLN', 'HIS', 'PRO', 'SER', 'ASN', 'VAL', 'LEU', 'HIS', 'PHE', 'PHE', 'ASN',
                                 'ALA', 'PRO', 'LEU', 'GLU', 'VAL', 'THR', 'GLU', 'GLU', 'ASN', 'PHE', 'PHE', 'GLU',
                                 'ILE', 'CYS', 'ASP', 'GLU', 'LEU', 'GLY', 'VAL', 'LYS', 'ARG', 'PRO', 'THR', 'SER',
                                 'VAL', 'LYS', 'VAL', 'PHE', 'SER', 'GLY', 'LYS', 'SER', 'GLU', 'ARG', 'SER', 'SER',
                                 'SER', 'GLY', 'LEU', 'LEU', 'GLU', 'TRP', 'ASP', 'SER', 'LYS', 'SER', 'ASP', 'ALA',
                                 'LEU', 'GLU', 'THR', 'LEU', 'GLY', 'PHE', 'LEU', 'ASN', 'HIS', 'TYR', 'GLN', 'MET',
                                 'LYS', 'ASN', 'PRO', 'ASN', 'GLY', 'PRO', 'TYR', 'PRO', 'TYR', 'THR', 'LEU', 'LYS',
                                 'LEU', 'CYS', 'PHE', 'SER', 'THR', 'ALA', 'GLN', 'HIS', 'ALA', 'SER'],
                           '2': ['A', 'C', 'A', 'C', 'A'], '3': ['A', 'C', 'A', 'C', 'A']}])
        entry = pynmrstar.Entry.from_file('data/2l9r.str')
        # extract polymer sequence from nef_molecular_system category
        self.assertEqual(bt.get_nmrstar_seq(entry['nef_molecular_system'], lp_category='Chem_comp_assembly')[0],
                         [{'1': ['MET', 'GLY', 'HIS', 'HIS', 'HIS', 'HIS', 'HIS', 'HIS', 'SER', 'HIS',
                                 'MET', 'SER', 'HIS', 'THR', 'GLN', 'VAL', 'ILE', 'GLU', 'LEU', 'GLU',
                                 'ARG', 'LYS', 'PHE', 'SER', 'HIS', 'GLN', 'LYS', 'TYR', 'LEU', 'SER',
                                 'ALA', 'PRO', 'GLU', 'ARG', 'ALA', 'HIS', 'LEU', 'ALA', 'LYS', 'ASN',
                                 'LEU', 'LYS', 'LEU', 'THR', 'GLU', 'THR', 'GLN', 'VAL', 'LYS', 'ILE',
                                 'TRP', 'PHE', 'GLN', 'ASN', 'ARG', 'ARG', 'TYR', 'LYS', 'THR', 'LYS',
                                 'ARG', 'LYS', 'GLN', 'LEU', 'SER', 'SER', 'GLU', 'LEU', 'GLY']}])
        self.assertEqual(bt.get_nmrstar_seq(entry['nef_molecular_system'], lp_category='Chem_comp_assembly')[1],
                         [{'1': [i for i in range(1, 70)]}])
        # extract polymer sequence from the first cs loop in nef_chemical_shift_list category
        cs_loops = entry.get_saveframes_by_category('assigned_chemical_shifts')
        self.assertEqual(len(cs_loops), 1) # assert single cs loop
        self.assertEqual(bt.get_nmrstar_seq(cs_loops[0], lp_category='Atom_chem_shift')[0], # select the first cs loop by input sta_data
                         [{'1': ['HIS',
                                 'MET', 'SER', 'HIS', 'THR', 'GLN', 'VAL', 'ILE', 'GLU', 'LEU', 'GLU',
                                 'ARG', 'LYS', 'PHE', 'SER', 'HIS', 'GLN', 'LYS', 'TYR', 'LEU', 'SER',
                                 'ALA', 'PRO', 'GLU', 'ARG', 'ALA', 'HIS', 'LEU', 'ALA', 'LYS', 'ASN',
                                 'LEU', 'LYS', 'LEU', 'THR', 'GLU', 'THR', 'GLN', 'VAL', 'LYS', 'ILE',
                                 'TRP', 'PHE', 'GLN', 'ASN', 'ARG', 'ARG', 'TYR', 'LYS', 'THR', 'LYS',
                                 'ARG', 'LYS', 'GLN', 'LEU', 'SER', 'SER', 'GLU', 'LEU', 'GLY']}])
        self.assertEqual(bt.get_nmrstar_seq(cs_loops[0], lp_category='Atom_chem_shift')[1], # select the first cs loop by input sta_data
                         [{'1': [i for i in range(10, 70)]}])
        # extract polymer sequence from nef_distant_restraint_list category
        self.assertEqual(bt.get_nmrstar_seq(entry['nef_distance_restraint_list_distance_constraint_list'], lp_category='Gen_dist_constraint')[0],
                         [{'1': ['HIS',
                                 'MET', 'SER', 'HIS', 'THR', 'GLN', 'VAL', 'ILE', 'GLU', 'LEU', 'GLU',
                                 'ARG', 'LYS', 'PHE', 'SER', 'HIS', 'GLN', 'LYS', 'TYR', 'LEU', 'SER',
                                 'ALA', 'PRO', 'GLU', 'ARG', 'ALA', 'HIS', 'LEU', 'ALA', 'LYS', 'ASN',
                                 'LEU', 'LYS', 'LEU', 'THR', 'GLU', 'THR', 'GLN', 'VAL', 'LYS', 'ILE',
                                 'TRP', 'PHE', 'GLN', 'ASN', 'ARG', 'ARG', 'TYR', 'LYS', 'THR', 'LYS',
                                 'ARG', 'LYS', 'GLN', 'LEU', 'SER', 'SER', 'GLU', 'LEU', 'GLY']}])
        self.assertEqual(bt.get_nmrstar_seq(entry['nef_distance_restraint_list_distance_constraint_list'], lp_category='Gen_dist_constraint')[1],
                         [{'1': [i for i in range(10, 70)]}])

    def test_validate_atom(self):
        bt = NEFT.NEFTranslator()
        dat = pynmrstar.Entry.from_file('data/2mqq.nef')
        self.assertEqual(len(bt.validate_atom(dat, 'nef_chemical_shift', 'sequence_code', 'residue_name', 'atom_name')),
                         567)
        self.assertEqual(
            len(bt.validate_atom(dat, 'nef_distance_restraint', 'sequence_code_1', 'residue_name_1', 'atom_name_1')),
            2960)
        self.assertEqual(
            len(bt.validate_atom(dat, 'nef_distance_restraint', 'sequence_code_2', 'residue_name_2', 'atom_name_2')),
            3147)
        dat = pynmrstar.Entry.from_file('data/2mqq.str')
        self.assertEqual(len(bt.validate_atom(dat)), 0)
        self.assertEqual(len(bt.validate_atom(dat, 'Gen_dist_constraint', 'Comp_index_ID_1', 'Comp_ID_1', 'Atom_ID_1')),
                         0)
        self.assertEqual(len(bt.validate_atom(dat, 'Gen_dist_constraint', 'Comp_index_ID_2', 'Comp_ID_2', 'Atom_ID_2')),
                         0)

    def test_get_nmrstar_tag(self):
        bt = NEFT.NEFTranslator()
        self.assertEqual(bt.get_nmrstar_tag('_nef_program_script.program_name'),
                         ['_Software_applied_methods.Software_name', '_Software_applied_methods.Software_name'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_program_script.script_name'),
                         ['_Software_applied_methods.Script_name', '_Software_applied_methods.Script_name'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_program_script.script'),
                         ['_Software_applied_methods.Script', '_Software_applied_methods.Script'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_sequence.index'),
                         ['_Chem_comp_assembly.NEF_index', '_Chem_comp_assembly.NEF_index'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_sequence.chain_code'),
                         ['_Chem_comp_assembly.Auth_asym_ID', '_Chem_comp_assembly.Entity_assembly_ID'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_sequence.sequence_code'),
                         ['_Chem_comp_assembly.Auth_seq_ID', '_Chem_comp_assembly.Comp_index_ID'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_sequence.residue_name'),
                         ['_Chem_comp_assembly.Auth_comp_ID', '_Chem_comp_assembly.Comp_ID'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_sequence.linking'),
                         ['_Chem_comp_assembly.Sequence_linking', '_Chem_comp_assembly.Sequence_linking'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_sequence.residue_variant'),
                         ['_Chem_comp_assembly.Auth_variant_ID', '_Chem_comp_assembly.Auth_variant_ID'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_sequence.cis_peptide'),
                         ['_Chem_comp_assembly.Cis_residue', '_Chem_comp_assembly.Cis_residue'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_chemical_shift.chain_code'),
                         ['_Atom_chem_shift.Auth_asym_ID', '_Atom_chem_shift.Entity_assembly_ID'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_chemical_shift.sequence_code'),
                         ['_Atom_chem_shift.Auth_seq_ID', '_Atom_chem_shift.Comp_index_ID'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_chemical_shift.residue_name'),
                         ['_Atom_chem_shift.Auth_comp_ID', '_Atom_chem_shift.Comp_ID'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_chemical_shift.atom_name'),
                         ['_Atom_chem_shift.Auth_atom_ID', '_Atom_chem_shift.Atom_ID'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_chemical_shift.value'),
                         ['_Atom_chem_shift.Val', '_Atom_chem_shift.Val'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_chemical_shift.value_uncertainty'),
                         ['_Atom_chem_shift.Val_err', '_Atom_chem_shift.Val_err'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_chemical_shift.element'),
                         ['_Atom_chem_shift.Atom_type', '_Atom_chem_shift.Atom_type'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_chemical_shift.isotope_number'),
                         ['_Atom_chem_shift.Atom_isotope_number', '_Atom_chem_shift.Atom_isotope_number'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_chemical_shift.chain_code'),
                         ['_Atom_chem_shift.Auth_asym_ID', '_Atom_chem_shift.Entity_assembly_ID'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_chemical_shift.sequence_code'),
                         ['_Atom_chem_shift.Auth_seq_ID', '_Atom_chem_shift.Comp_index_ID'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_chemical_shift.residue_name'),
                         ['_Atom_chem_shift.Auth_comp_ID', '_Atom_chem_shift.Comp_ID'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_chemical_shift.atom_name'),
                         ['_Atom_chem_shift.Auth_atom_ID', '_Atom_chem_shift.Atom_ID'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_chemical_shift.value'),
                         ['_Atom_chem_shift.Val', '_Atom_chem_shift.Val'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_chemical_shift.value_uncertainty'),
                         ['_Atom_chem_shift.Val_err', '_Atom_chem_shift.Val_err'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_chemical_shift.element'),
                         ['_Atom_chem_shift.Atom_type', '_Atom_chem_shift.Atom_type'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_chemical_shift.isotope_number'),
                         ['_Atom_chem_shift.Atom_isotope_number', '_Atom_chem_shift.Atom_isotope_number'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_chemical_shift.chain_code'),
                         ['_Atom_chem_shift.Auth_asym_ID', '_Atom_chem_shift.Entity_assembly_ID'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_chemical_shift.sequence_code'),
                         ['_Atom_chem_shift.Auth_seq_ID', '_Atom_chem_shift.Comp_index_ID'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_chemical_shift.residue_name'),
                         ['_Atom_chem_shift.Auth_comp_ID', '_Atom_chem_shift.Comp_ID'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_chemical_shift.atom_name'),
                         ['_Atom_chem_shift.Auth_atom_ID', '_Atom_chem_shift.Atom_ID'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_chemical_shift.value'),
                         ['_Atom_chem_shift.Val', '_Atom_chem_shift.Val'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_chemical_shift.value_uncertainty'),
                         ['_Atom_chem_shift.Val_err', '_Atom_chem_shift.Val_err'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_chemical_shift.element'),
                         ['_Atom_chem_shift.Atom_type', '_Atom_chem_shift.Atom_type'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_chemical_shift.isotope_number'),
                         ['_Atom_chem_shift.Atom_isotope_number', '_Atom_chem_shift.Atom_isotope_number'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_distance_restraint.index'),
                         ['_Gen_dist_constraint.Index_ID', '_Gen_dist_constraint.Index_ID'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_distance_restraint.restraint_id'),
                         ['_Gen_dist_constraint.ID', '_Gen_dist_constraint.ID'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_distance_restraint.restraint_combination_id'),
                         ['_Gen_dist_constraint.Combination_ID', '_Gen_dist_constraint.Combination_ID'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_distance_restraint.chain_code_1'),
                         ['_Gen_dist_constraint.Auth_asym_ID_1', '_Gen_dist_constraint.Entity_assembly_ID_1'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_distance_restraint.sequence_code_1'),
                         ['_Gen_dist_constraint.Auth_seq_ID_1', '_Gen_dist_constraint.Comp_index_ID_1'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_distance_restraint.residue_name_1'),
                         ['_Gen_dist_constraint.Auth_comp_ID_1', '_Gen_dist_constraint.Comp_ID_1'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_distance_restraint.atom_name_1'),
                         ['_Gen_dist_constraint.Auth_atom_ID_1', '_Gen_dist_constraint.Atom_ID_1'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_distance_restraint.chain_code_2'),
                         ['_Gen_dist_constraint.Auth_asym_ID_2', '_Gen_dist_constraint.Entity_assembly_ID_2'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_distance_restraint.sequence_code_2'),
                         ['_Gen_dist_constraint.Auth_seq_ID_2', '_Gen_dist_constraint.Comp_index_ID_2'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_distance_restraint.residue_name_2'),
                         ['_Gen_dist_constraint.Auth_comp_ID_2', '_Gen_dist_constraint.Comp_ID_2'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_distance_restraint.atom_name_2'),
                         ['_Gen_dist_constraint.Auth_atom_ID_2', '_Gen_dist_constraint.Atom_ID_2'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_distance_restraint.weight'),
                         ['_Gen_dist_constraint.Weight', '_Gen_dist_constraint.Weight'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_distance_restraint.target_value'),
                         ['_Gen_dist_constraint.Target_val', '_Gen_dist_constraint.Target_val'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_distance_restraint.target_value_uncertainty'),
                         ['_Gen_dist_constraint.Target_val_uncertainty', '_Gen_dist_constraint.Target_val_uncertainty'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_distance_restraint.lower_linear_limit'),
                         ['_Gen_dist_constraint.Lower_linear_limit', '_Gen_dist_constraint.Lower_linear_limit'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_distance_restraint.lower_limit'),
                         ['_Gen_dist_constraint.Distance_lower_bound_val',
                          '_Gen_dist_constraint.Distance_lower_bound_val'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_distance_restraint.upper_limit'),
                         ['_Gen_dist_constraint.Distance_upper_bound_val',
                          '_Gen_dist_constraint.Distance_upper_bound_val'])
        self.assertEqual(bt.get_nmrstar_tag('_nef_distance_restraint.upper_linear_limit'),
                         ['_Gen_dist_constraint.Upper_linear_limit', '_Gen_dist_constraint.Upper_linear_limit'])

    def test_get_nmrstar_loop_tags(self):
        bt = NEFT.NEFTranslator()
        self.assertEqual(bt.get_nmrstar_loop_tags(
            ['_nef_program_script.program_name', '_nef_program_script.script_name', '_nef_program_script.script']),
            ['_Software_applied_methods.Software_name', '_Software_applied_methods.Script_name',
             '_Software_applied_methods.Script'])
        self.assertEqual(bt.get_nmrstar_loop_tags(
            ['_nef_sequence.index', '_nef_sequence.chain_code', '_nef_sequence.sequence_code',
             '_nef_sequence.residue_name', '_nef_sequence.linking', '_nef_sequence.residue_variant',
             '_nef_sequence.cis_peptide']), ['_Chem_comp_assembly.NEF_index', '_Chem_comp_assembly.Auth_asym_ID',
                                             '_Chem_comp_assembly.Auth_seq_ID', '_Chem_comp_assembly.Auth_comp_ID',
                                             '_Chem_comp_assembly.Sequence_linking',
                                             '_Chem_comp_assembly.Auth_variant_ID', '_Chem_comp_assembly.Cis_residue',
                                             '_Chem_comp_assembly.Entity_assembly_ID',
                                             '_Chem_comp_assembly.Comp_index_ID', '_Chem_comp_assembly.Comp_ID'])
        self.assertEqual(bt.get_nmrstar_loop_tags(
            ['_nef_chemical_shift.chain_code', '_nef_chemical_shift.sequence_code', '_nef_chemical_shift.residue_name',
             '_nef_chemical_shift.atom_name', '_nef_chemical_shift.value', '_nef_chemical_shift.value_uncertainty',
             '_nef_chemical_shift.element', '_nef_chemical_shift.isotope_number']),
            ['_Atom_chem_shift.Auth_asym_ID', '_Atom_chem_shift.Auth_seq_ID',
             '_Atom_chem_shift.Auth_comp_ID', '_Atom_chem_shift.Auth_atom_ID', '_Atom_chem_shift.Val',
             '_Atom_chem_shift.Val_err', '_Atom_chem_shift.Atom_type',
             '_Atom_chem_shift.Atom_isotope_number', '_Atom_chem_shift.Entity_assembly_ID',
             '_Atom_chem_shift.Comp_index_ID', '_Atom_chem_shift.Comp_ID', '_Atom_chem_shift.Atom_ID',
             '_Atom_chem_shift.Ambiguity_code', '_Atom_chem_shift.Ambiguity_set_ID',
             '_Atom_chem_shift.Assigned_chem_shift_list_ID'])
        self.assertEqual(bt.get_nmrstar_loop_tags(
            ['_nef_chemical_shift.chain_code', '_nef_chemical_shift.sequence_code', '_nef_chemical_shift.residue_name',
             '_nef_chemical_shift.atom_name', '_nef_chemical_shift.value', '_nef_chemical_shift.value_uncertainty',
             '_nef_chemical_shift.element', '_nef_chemical_shift.isotope_number']),
            ['_Atom_chem_shift.Auth_asym_ID', '_Atom_chem_shift.Auth_seq_ID',
             '_Atom_chem_shift.Auth_comp_ID', '_Atom_chem_shift.Auth_atom_ID', '_Atom_chem_shift.Val',
             '_Atom_chem_shift.Val_err', '_Atom_chem_shift.Atom_type',
             '_Atom_chem_shift.Atom_isotope_number', '_Atom_chem_shift.Entity_assembly_ID',
             '_Atom_chem_shift.Comp_index_ID', '_Atom_chem_shift.Comp_ID', '_Atom_chem_shift.Atom_ID',
             '_Atom_chem_shift.Ambiguity_code', '_Atom_chem_shift.Ambiguity_set_ID',
             '_Atom_chem_shift.Assigned_chem_shift_list_ID'])
        self.assertEqual(bt.get_nmrstar_loop_tags(
            ['_nef_chemical_shift.chain_code', '_nef_chemical_shift.sequence_code', '_nef_chemical_shift.residue_name',
             '_nef_chemical_shift.atom_name', '_nef_chemical_shift.value', '_nef_chemical_shift.value_uncertainty',
             '_nef_chemical_shift.element', '_nef_chemical_shift.isotope_number']),
            ['_Atom_chem_shift.Auth_asym_ID', '_Atom_chem_shift.Auth_seq_ID',
             '_Atom_chem_shift.Auth_comp_ID', '_Atom_chem_shift.Auth_atom_ID', '_Atom_chem_shift.Val',
             '_Atom_chem_shift.Val_err', '_Atom_chem_shift.Atom_type',
             '_Atom_chem_shift.Atom_isotope_number', '_Atom_chem_shift.Entity_assembly_ID',
             '_Atom_chem_shift.Comp_index_ID', '_Atom_chem_shift.Comp_ID', '_Atom_chem_shift.Atom_ID',
             '_Atom_chem_shift.Ambiguity_code', '_Atom_chem_shift.Ambiguity_set_ID',
             '_Atom_chem_shift.Assigned_chem_shift_list_ID'])
        self.assertEqual(bt.get_nmrstar_loop_tags(['_nef_distance_restraint.index',
                                                   '_nef_distance_restraint.restraint_id',
                                                   '_nef_distance_restraint.restraint_combination_id',
                                                   '_nef_distance_restraint.chain_code_1',
                                                   '_nef_distance_restraint.sequence_code_1',
                                                   '_nef_distance_restraint.residue_name_1',
                                                   '_nef_distance_restraint.atom_name_1',
                                                   '_nef_distance_restraint.chain_code_2',
                                                   '_nef_distance_restraint.sequence_code_2',
                                                   '_nef_distance_restraint.residue_name_2',
                                                   '_nef_distance_restraint.atom_name_2',
                                                   '_nef_distance_restraint.weight',
                                                   '_nef_distance_restraint.target_value',
                                                   '_nef_distance_restraint.target_value_uncertainty',
                                                   '_nef_distance_restraint.lower_linear_limit',
                                                   '_nef_distance_restraint.lower_limit',
                                                   '_nef_distance_restraint.upper_limit',
                                                   '_nef_distance_restraint.upper_linear_limit']),
                         ['_Gen_dist_constraint.Index_ID', '_Gen_dist_constraint.ID',
                          '_Gen_dist_constraint.Combination_ID', '_Gen_dist_constraint.Auth_asym_ID_1',
                          '_Gen_dist_constraint.Auth_seq_ID_1', '_Gen_dist_constraint.Auth_comp_ID_1',
                          '_Gen_dist_constraint.Auth_atom_ID_1', '_Gen_dist_constraint.Auth_asym_ID_2',
                          '_Gen_dist_constraint.Auth_seq_ID_2', '_Gen_dist_constraint.Auth_comp_ID_2',
                          '_Gen_dist_constraint.Auth_atom_ID_2', '_Gen_dist_constraint.Weight',
                          '_Gen_dist_constraint.Target_val', '_Gen_dist_constraint.Target_val_uncertainty',
                          '_Gen_dist_constraint.Lower_linear_limit', '_Gen_dist_constraint.Distance_lower_bound_val',
                          '_Gen_dist_constraint.Distance_upper_bound_val', '_Gen_dist_constraint.Upper_linear_limit',
                          '_Gen_dist_constraint.Entity_assembly_ID_1', '_Gen_dist_constraint.Comp_index_ID_1',
                          '_Gen_dist_constraint.Comp_ID_1', '_Gen_dist_constraint.Atom_ID_1',
                          '_Gen_dist_constraint.Entity_assembly_ID_2', '_Gen_dist_constraint.Comp_index_ID_2',
                          '_Gen_dist_constraint.Comp_ID_2', '_Gen_dist_constraint.Atom_ID_2',
                          '_Gen_dist_constraint.Member_logic_code','_Gen_dist_constraint.Gen_dist_constraint_list_ID'])

    def test_get_nmrstar_atom(self):
        bt = NEFT.NEFTranslator()
        self.assertEqual(bt.get_nmrstar_atom('CYS', 'HB%'), ('HB', ['HB2', 'HB3'], 1))
        self.assertEqual(bt.get_nmrstar_atom('TRP', 'CE%'), ('CE', ['CE2', 'CE3'], 1))
        self.assertEqual(bt.get_nmrstar_atom('TRP', 'CEX'), ('CE', ['CE2'], 2))
        self.assertEqual(bt.get_nmrstar_atom('TRP', 'CEY'), ('CE', ['CE3'], 2))
        self.assertEqual(bt.get_nmrstar_atom('LEU', 'HDY%'), ('HD', ['HD21', 'HD22', 'HD23'], 2))
        self.assertEqual(bt.get_nmrstar_atom('LEU', 'HD1%'), ('HD1', ['HD11', 'HD12', 'HD13'], 1))

    def test_translate_cs_row(self):
        bt = NEFT.NEFTranslator()
        bt.seqDict = {('A', '372'): (1, 1), ('A', '373'): (1, 2), ('A', '374'): (1, 3), ('A', '375'): (1, 4),
                      ('A', '376'): (1, 5), ('A', '377'): (1, 6), ('A', '378'): (1, 7), ('A', '379'): (1, 8),
                      ('A', '380'): (1, 9), ('A', '381'): (1, 10), ('A', '382'): (1, 11), ('A', '383'): (1, 12),
                      ('A', '384'): (1, 13), ('A', '385'): (1, 14), ('A', '386'): (1, 15), ('A', '387'): (1, 16),
                      ('A', '388'): (1, 17), ('A', '389'): (1, 18), ('A', '390'): (1, 19), ('A', '391'): (1, 20),
                      ('A', '392'): (1, 21), ('A', '393'): (1, 22), ('A', '394'): (1, 23), ('A', '395'): (1, 24),
                      ('A', '396'): (1, 25), ('A', '397'): (1, 26), ('A', '398'): (1, 27), ('A', '399'): (1, 28),
                      ('A', '400'): (1, 29), ('A', '401'): (1, 30), ('A', '402'): (1, 31), ('A', '403'): (1, 32),
                      ('A', '404'): (1, 33), ('A', '405'): (1, 34), ('A', '406'): (1, 35), ('A', '407'): (1, 36),
                      ('A', '408'): (1, 37), ('A', '409'): (1, 38), ('A', '410'): (1, 39), ('A', '411'): (1, 40),
                      ('A', '412'): (1, 41), ('A', '413'): (1, 42), ('A', '414'): (1, 43), ('A', '415'): (1, 44),
                      ('A', '416'): (1, 45), ('A', '417'): (1, 46), ('A', '418'): (1, 47), ('A', '419'): (1, 48),
                      ('A', '420'): (1, 49), ('A', '421'): (1, 50), ('A', '422'): (1, 51), ('A', '423'): (1, 52),
                      ('A', '424'): (1, 53), ('A', '425'): (1, 54), ('A', '426'): (1, 55), ('A', '427'): (1, 56),
                      ('A', '428'): (1, 57), ('A', '429'): (1, 58), ('A', '430'): (1, 59), ('A', '431'): (1, 60),
                      ('A', '432'): (1, 61), ('A', '433'): (1, 62), ('A', '434'): (1, 63), ('A', '435'): (1, 64),
                      ('A', '436'): (1, 65), ('A', '437'): (1, 66), ('A', '438'): (1, 67), ('A', '439'): (1, 68),
                      ('A', '440'): (1, 69), ('A', '441'): (1, 70), ('A', '442'): (1, 71), ('A', '443'): (1, 72),
                      ('A', '444'): (1, 73), ('A', '445'): (1, 74), ('A', '446'): (1, 75), ('A', '447'): (1, 76),
                      ('A', '448'): (1, 77), ('A', '449'): (1, 78), ('A', '450'): (1, 79), ('A', '451'): (1, 80),
                      ('A', '452'): (1, 81), ('A', '453'): (1, 82), ('A', '454'): (1, 83), ('A', '455'): (1, 84),
                      ('A', '456'): (1, 85), ('A', '457'): (1, 86), ('A', '458'): (1, 87), ('A', '459'): (1, 88),
                      ('A', '460'): (1, 89), ('A', '461'): (1, 90), ('A', '462'): (1, 91), ('A', '463'): (1, 92),
                      ('A', '464'): (1, 93), ('A', '465'): (1, 94), ('A', '466'): (1, 95), ('A', '467'): (1, 96),
                      ('A', '468'): (1, 97), ('A', '469'): (1, 98), ('A', '470'): (1, 99), ('A', '471'): (1, 100),
                      ('A', '472'): (1, 101), ('A', '473'): (1, 102), ('A', '474'): (1, 103), ('A', '475'): (1, 104),
                      ('A', '476'): (1, 105), ('A', '477'): (1, 106), ('A', '478'): (1, 107), ('A', '479'): (1, 108),
                      ('A', '480'): (1, 109), ('A', '481'): (1, 110), ('A', '482'): (1, 111), ('A', '483'): (1, 112),
                      ('A', '484'): (1, 113), ('A', '485'): (1, 114), ('A', '486'): (1, 115), ('A', '487'): (1, 116),
                      ('A', '488'): (1, 117), ('A', '489'): (1, 118), ('A', '490'): (1, 119), ('A', '491'): (1, 120),
                      ('A', '492'): (1, 121), ('A', '493'): (1, 122), ('A', '494'): (1, 123), ('A', '495'): (1, 124),
                      ('A', '496'): (1, 125), ('A', '497'): (1, 126), ('A', '498'): (1, 127), ('A', '499'): (1, 128),
                      ('A', '500'): (1, 129), ('A', '501'): (1, 130), ('A', '502'): (1, 131), ('A', '503'): (1, 132),
                      ('A', '504'): (1, 133), ('A', '505'): (1, 134), ('A', '506'): (1, 135), ('A', '507'): (1, 136),
                      ('A', '508'): (1, 137), ('A', '509'): (1, 138), ('A', '510'): (1, 139), ('A', '511'): (1, 140),
                      ('A', '512'): (1, 141), ('A', '513'): (1, 142), ('A', '514'): (1, 143), ('A', '515'): (1, 144),
                      ('A', '516'): (1, 145), ('A', '517'): (1, 146), ('A', '518'): (1, 147), ('A', '519'): (1, 148),
                      ('A', '520'): (1, 149), ('A', '521'): (1, 150), ('A', '522'): (1, 151), ('A', '523'): (1, 152),
                      ('A', '524'): (1, 153), ('A', '525'): (1, 154), ('A', '526'): (1, 155), ('A', '527'): (1, 156),
                      ('A', '528'): (1, 157), ('A', '529'): (1, 158), ('A', '530'): (1, 159), ('A', '531'): (1, 160),
                      ('A', '532'): (1, 161), ('A', '533'): (1, 162), ('A', '534'): (1, 163), ('A', '535'): (1, 164),
                      ('A', '536'): (1, 165), ('A', '537'): (1, 166), ('A', '538'): (1, 167), ('A', '539'): (1, 168),
                      ('A', '540'): (1, 169), ('A', '541'): (1, 170), ('A', '542'): (1, 171), ('A', '543'): (1, 172),
                      ('A', '544'): (1, 173), ('A', '545'): (1, 174), ('A', '546'): (1, 175), ('A', '547'): (1, 176),
                      ('A', '548'): (1, 177), ('A', '549'): (1, 178), ('A', '550'): (1, 179), ('A', '551'): (1, 180),
                      ('A', '552'): (1, 181), ('A', '553'): (1, 182), ('A', '554'): (1, 183), ('A', '555'): (1, 184),
                      ('A', '556'): (1, 185), ('A', '557'): (1, 186), ('A', '558'): (1, 187), ('A', '559'): (1, 188),
                      ('A', '560'): (1, 189), ('A', '561'): (1, 190), ('A', '562'): (1, 191), ('A', '563'): (1, 192),
                      ('A', '564'): (1, 193), ('A', '565'): (1, 194), ('A', '566'): (1, 195), ('A', '567'): (1, 196),
                      ('A', '568'): (1, 197), ('A', '569'): (1, 198), ('A', '570'): (1, 199), ('A', '571'): (1, 200),
                      ('A', '572'): (1, 201), ('A', '573'): (1, 202), ('A', '574'): (1, 203), ('A', '575'): (1, 204),
                      ('A', '576'): (1, 205), ('A', '577'): (1, 206), ('A', '578'): (1, 207), ('A', '579'): (1, 208),
                      ('A', '580'): (1, 209), ('A', '581'): (1, 210), ('A', '582'): (1, 211), ('A', '583'): (1, 212),
                      ('A', '584'): (1, 213), ('A', '585'): (1, 214), ('A', '586'): (1, 215), ('B', '1'): (2, 1),
                      ('B', '2'): (2, 2), ('B', '3'): (2, 3), ('B', '4'): (2, 4), ('B', '5'): (2, 5),
                      ('C', '1'): (3, 1), ('C', '2'): (3, 2), ('C', '3'): (3, 3), ('C', '4'): (3, 4),
                      ('C', '5'): (3, 5)}
        input_tags = ['_nef_chemical_shift.chain_code', '_nef_chemical_shift.sequence_code',
                      '_nef_chemical_shift.residue_name', '_nef_chemical_shift.atom_name',
                      '_nef_chemical_shift.value', '_nef_chemical_shift.value_uncertainty',
                      '_nef_chemical_shift.element', '_nef_chemical_shift.isotope_number']
        output_tags = ['_Atom_chem_shift.Auth_asym_ID', '_Atom_chem_shift.Auth_seq_ID',
                       '_Atom_chem_shift.Auth_comp_ID', '_Atom_chem_shift.Auth_atom_ID',
                       '_Atom_chem_shift.Val', '_Atom_chem_shift.Val_err', '_Atom_chem_shift.Atom_type',
                       '_Atom_chem_shift.Atom_isotope_number', '_Atom_chem_shift.Entity_assembly_ID',
                       '_Atom_chem_shift.Comp_index_ID', '_Atom_chem_shift.Comp_ID', '_Atom_chem_shift.Atom_ID',
                       '_Atom_chem_shift.Ambiguity_code', '_Atom_chem_shift.Ambiguity_set_ID',
                       '_Atom_chem_shift.Assigned_chem_shift_list_ID']
        data = ['A', '484', 'THR', 'N', '108.193', '0.4', 'N', '15']
        data_out = [['A', '484', 'THR', 'N', '108.193', '0.4', 'N', '15', 1, 113, 'THR', 'N', 1, '.', None]]
        self.assertEqual(bt.translate_cs_row(input_tags, output_tags, data), data_out)
        data = ['A', '488', 'ALA', 'HB%', '1.625', '0.02', 'H', '1']
        data_out = [['A', '488', 'ALA', 'HB%', '1.625', '0.02', 'H', '1', 1, 117, 'ALA', u'HB1', 1, '.', None],
                    ['A', '488', 'ALA', 'HB%', '1.625', '0.02', 'H', '1', 1, 117, 'ALA', u'HB2', 1, '.', None],
                    ['A', '488', 'ALA', 'HB%', '1.625', '0.02', 'H', '1', 1, 117, 'ALA', u'HB3', 1, '.', None]]
        self.assertEqual(bt.translate_cs_row(input_tags, output_tags, data), data_out)
        data = ['A', '493', 'ILE', 'HD1%', '0.996', '0.02', 'H', '1']
        data_out = [['A', '493', 'ILE', 'HD1%', '0.996', '0.02', 'H', '1', 1, 122, 'ILE', u'HD11', 1, '.', None],
                    ['A', '493', 'ILE', 'HD1%', '0.996', '0.02', 'H', '1', 1, 122, 'ILE', u'HD12', 1, '.', None],
                    ['A', '493', 'ILE', 'HD1%', '0.996', '0.02', 'H', '1', 1, 122, 'ILE', u'HD13', 1, '.', None]]
        self.assertEqual(bt.translate_cs_row(input_tags, output_tags, data), data_out)
        data = ['A', '493', 'ILE', 'HG1x', '1.627', '0.02', 'H', '1']
        data_out = [['A', '493', 'ILE', 'HG1x', '1.627', '0.02', 'H', '1', 1, 122, 'ILE', u'HG12', 2, '.', None]]
        self.assertEqual(bt.translate_cs_row(input_tags, output_tags, data), data_out)
        data = ['A', '493', 'ILE', 'HG1y', '1.536', '0.02', 'H', '1']
        data_out = [['A', '493', 'ILE', 'HG1y', '1.536', '0.02', 'H', '1', 1, 122, 'ILE', u'HG13', 2, '.', None]]
        self.assertEqual(bt.translate_cs_row(input_tags, output_tags, data), data_out)
        data = ['A', '493', 'ILE', 'HG2%', '0.859', '0.02', 'H', '1']
        data_out = [['A', '493', 'ILE', 'HG2%', '0.859', '0.02', 'H', '1', 1, 122, 'ILE', u'HG21', 1, '.', None],
                    ['A', '493', 'ILE', 'HG2%', '0.859', '0.02', 'H', '1', 1, 122, 'ILE', u'HG22', 1, '.', None],
                    ['A', '493', 'ILE', 'HG2%', '0.859', '0.02', 'H', '1', 1, 122, 'ILE', u'HG23', 1, '.', None]]
        self.assertEqual(bt.translate_cs_row(input_tags, output_tags, data), data_out)

    def test_get_residue_identifier(self):
        bt = NEFT.NEFTranslator()
        inputtags = ['_nef_dihedral_restraint.index', '_nef_dihedral_restraint.restraint_id',
                     '_nef_dihedral_restraint.restraint_combination_id', '_nef_dihedral_restraint.chain_code_1',
                     '_nef_dihedral_restraint.sequence_code_1', '_nef_dihedral_restraint.residue_name_1',
                     '_nef_dihedral_restraint.atom_name_1', '_nef_dihedral_restraint.chain_code_2',
                     '_nef_dihedral_restraint.sequence_code_2', '_nef_dihedral_restraint.residue_name_2',
                     '_nef_dihedral_restraint.atom_name_2', '_nef_dihedral_restraint.chain_code_3',
                     '_nef_dihedral_restraint.sequence_code_3', '_nef_dihedral_restraint.residue_name_3',
                     '_nef_dihedral_restraint.atom_name_3', '_nef_dihedral_restraint.chain_code_4',
                     '_nef_dihedral_restraint.sequence_code_4', '_nef_dihedral_restraint.residue_name_4',
                     '_nef_dihedral_restraint.atom_name_4', '_nef_dihedral_restraint.weight',
                     '_nef_dihedral_restraint.target_value', '_nef_dihedral_restraint.target_value_uncertainty',
                     '_nef_dihedral_restraint.lower_linear_limit', '_nef_dihedral_restraint.lower_limit',
                     '_nef_dihedral_restraint.upper_limit', '_nef_dihedral_restraint.upper_linear_limit',
                     '_nef_dihedral_restraint.name']
        outtags = [['_nef_dihedral_restraint.chain_code_1', '_nef_dihedral_restraint.sequence_code_1'],
                   ['_nef_dihedral_restraint.chain_code_2', '_nef_dihedral_restraint.sequence_code_2'],
                   ['_nef_dihedral_restraint.chain_code_3', '_nef_dihedral_restraint.sequence_code_3'],
                   ['_nef_dihedral_restraint.chain_code_4', '_nef_dihedral_restraint.sequence_code_4']]
        self.assertEqual(bt.get_residue_identifier(inputtags), outtags)

    def test_translate_row(self):
        bt = NEFT.NEFTranslator()
        bt.seqDict = {('A', '372'): (1, 1), ('A', '373'): (1, 2), ('A', '374'): (1, 3), ('A', '375'): (1, 4),
                      ('A', '376'): (1, 5), ('A', '377'): (1, 6), ('A', '378'): (1, 7), ('A', '379'): (1, 8),
                      ('A', '380'): (1, 9), ('A', '381'): (1, 10), ('A', '382'): (1, 11), ('A', '383'): (1, 12),
                      ('A', '384'): (1, 13), ('A', '385'): (1, 14), ('A', '386'): (1, 15), ('A', '387'): (1, 16),
                      ('A', '388'): (1, 17), ('A', '389'): (1, 18), ('A', '390'): (1, 19), ('A', '391'): (1, 20),
                      ('A', '392'): (1, 21), ('A', '393'): (1, 22), ('A', '394'): (1, 23), ('A', '395'): (1, 24),
                      ('A', '396'): (1, 25), ('A', '397'): (1, 26), ('A', '398'): (1, 27), ('A', '399'): (1, 28),
                      ('A', '400'): (1, 29), ('A', '401'): (1, 30), ('A', '402'): (1, 31), ('A', '403'): (1, 32),
                      ('A', '404'): (1, 33), ('A', '405'): (1, 34), ('A', '406'): (1, 35), ('A', '407'): (1, 36),
                      ('A', '408'): (1, 37), ('A', '409'): (1, 38), ('A', '410'): (1, 39), ('A', '411'): (1, 40),
                      ('A', '412'): (1, 41), ('A', '413'): (1, 42), ('A', '414'): (1, 43), ('A', '415'): (1, 44),
                      ('A', '416'): (1, 45), ('A', '417'): (1, 46), ('A', '418'): (1, 47), ('A', '419'): (1, 48),
                      ('A', '420'): (1, 49), ('A', '421'): (1, 50), ('A', '422'): (1, 51), ('A', '423'): (1, 52),
                      ('A', '424'): (1, 53), ('A', '425'): (1, 54), ('A', '426'): (1, 55), ('A', '427'): (1, 56),
                      ('A', '428'): (1, 57), ('A', '429'): (1, 58), ('A', '430'): (1, 59), ('A', '431'): (1, 60),
                      ('A', '432'): (1, 61), ('A', '433'): (1, 62), ('A', '434'): (1, 63), ('A', '435'): (1, 64),
                      ('A', '436'): (1, 65), ('A', '437'): (1, 66), ('A', '438'): (1, 67), ('A', '439'): (1, 68),
                      ('A', '440'): (1, 69), ('A', '441'): (1, 70), ('A', '442'): (1, 71), ('A', '443'): (1, 72),
                      ('A', '444'): (1, 73), ('A', '445'): (1, 74), ('A', '446'): (1, 75), ('A', '447'): (1, 76),
                      ('A', '448'): (1, 77), ('A', '449'): (1, 78), ('A', '450'): (1, 79), ('A', '451'): (1, 80),
                      ('A', '452'): (1, 81), ('A', '453'): (1, 82), ('A', '454'): (1, 83), ('A', '455'): (1, 84),
                      ('A', '456'): (1, 85), ('A', '457'): (1, 86), ('A', '458'): (1, 87), ('A', '459'): (1, 88),
                      ('A', '460'): (1, 89), ('A', '461'): (1, 90), ('A', '462'): (1, 91), ('A', '463'): (1, 92),
                      ('A', '464'): (1, 93), ('A', '465'): (1, 94), ('A', '466'): (1, 95), ('A', '467'): (1, 96),
                      ('A', '468'): (1, 97), ('A', '469'): (1, 98), ('A', '470'): (1, 99), ('A', '471'): (1, 100),
                      ('A', '472'): (1, 101), ('A', '473'): (1, 102), ('A', '474'): (1, 103), ('A', '475'): (1, 104),
                      ('A', '476'): (1, 105), ('A', '477'): (1, 106), ('A', '478'): (1, 107), ('A', '479'): (1, 108),
                      ('A', '480'): (1, 109), ('A', '481'): (1, 110), ('A', '482'): (1, 111), ('A', '483'): (1, 112),
                      ('A', '484'): (1, 113), ('A', '485'): (1, 114), ('A', '486'): (1, 115), ('A', '487'): (1, 116),
                      ('A', '488'): (1, 117), ('A', '489'): (1, 118), ('A', '490'): (1, 119), ('A', '491'): (1, 120),
                      ('A', '492'): (1, 121), ('A', '493'): (1, 122), ('A', '494'): (1, 123), ('A', '495'): (1, 124),
                      ('A', '496'): (1, 125), ('A', '497'): (1, 126), ('A', '498'): (1, 127), ('A', '499'): (1, 128),
                      ('A', '500'): (1, 129), ('A', '501'): (1, 130), ('A', '502'): (1, 131), ('A', '503'): (1, 132),
                      ('A', '504'): (1, 133), ('A', '505'): (1, 134), ('A', '506'): (1, 135), ('A', '507'): (1, 136),
                      ('A', '508'): (1, 137), ('A', '509'): (1, 138), ('A', '510'): (1, 139), ('A', '511'): (1, 140),
                      ('A', '512'): (1, 141), ('A', '513'): (1, 142), ('A', '514'): (1, 143), ('A', '515'): (1, 144),
                      ('A', '516'): (1, 145), ('A', '517'): (1, 146), ('A', '518'): (1, 147), ('A', '519'): (1, 148),
                      ('A', '520'): (1, 149), ('A', '521'): (1, 150), ('A', '522'): (1, 151), ('A', '523'): (1, 152),
                      ('A', '524'): (1, 153), ('A', '525'): (1, 154), ('A', '526'): (1, 155), ('A', '527'): (1, 156),
                      ('A', '528'): (1, 157), ('A', '529'): (1, 158), ('A', '530'): (1, 159), ('A', '531'): (1, 160),
                      ('A', '532'): (1, 161), ('A', '533'): (1, 162), ('A', '534'): (1, 163), ('A', '535'): (1, 164),
                      ('A', '536'): (1, 165), ('A', '537'): (1, 166), ('A', '538'): (1, 167), ('A', '539'): (1, 168),
                      ('A', '540'): (1, 169), ('A', '541'): (1, 170), ('A', '542'): (1, 171), ('A', '543'): (1, 172),
                      ('A', '544'): (1, 173), ('A', '545'): (1, 174), ('A', '546'): (1, 175), ('A', '547'): (1, 176),
                      ('A', '548'): (1, 177), ('A', '549'): (1, 178), ('A', '550'): (1, 179), ('A', '551'): (1, 180),
                      ('A', '552'): (1, 181), ('A', '553'): (1, 182), ('A', '554'): (1, 183), ('A', '555'): (1, 184),
                      ('A', '556'): (1, 185), ('A', '557'): (1, 186), ('A', '558'): (1, 187), ('A', '559'): (1, 188),
                      ('A', '560'): (1, 189), ('A', '561'): (1, 190), ('A', '562'): (1, 191), ('A', '563'): (1, 192),
                      ('A', '564'): (1, 193), ('A', '565'): (1, 194), ('A', '566'): (1, 195), ('A', '567'): (1, 196),
                      ('A', '568'): (1, 197), ('A', '569'): (1, 198), ('A', '570'): (1, 199), ('A', '571'): (1, 200),
                      ('A', '572'): (1, 201), ('A', '573'): (1, 202), ('A', '574'): (1, 203), ('A', '575'): (1, 204),
                      ('A', '576'): (1, 205), ('A', '577'): (1, 206), ('A', '578'): (1, 207), ('A', '579'): (1, 208),
                      ('A', '580'): (1, 209), ('A', '581'): (1, 210), ('A', '582'): (1, 211), ('A', '583'): (1, 212),
                      ('A', '584'): (1, 213), ('A', '585'): (1, 214), ('A', '586'): (1, 215), ('B', '1'): (2, 1),
                      ('B', '2'): (2, 2), ('B', '3'): (2, 3), ('B', '4'): (2, 4), ('B', '5'): (2, 5),
                      ('C', '1'): (3, 1), ('C', '2'): (3, 2), ('C', '3'): (3, 3), ('C', '4'): (3, 4),
                      ('C', '5'): (3, 5)}
        intag = ['_nef_dihedral_restraint.index', '_nef_dihedral_restraint.restraint_id',
                 '_nef_dihedral_restraint.restraint_combination_id', '_nef_dihedral_restraint.chain_code_1',
                 '_nef_dihedral_restraint.sequence_code_1', '_nef_dihedral_restraint.residue_name_1',
                 '_nef_dihedral_restraint.atom_name_1', '_nef_dihedral_restraint.chain_code_2',
                 '_nef_dihedral_restraint.sequence_code_2', '_nef_dihedral_restraint.residue_name_2',
                 '_nef_dihedral_restraint.atom_name_2', '_nef_dihedral_restraint.chain_code_3',
                 '_nef_dihedral_restraint.sequence_code_3', '_nef_dihedral_restraint.residue_name_3',
                 '_nef_dihedral_restraint.atom_name_3', '_nef_dihedral_restraint.chain_code_4',
                 '_nef_dihedral_restraint.sequence_code_4', '_nef_dihedral_restraint.residue_name_4',
                 '_nef_dihedral_restraint.atom_name_4', '_nef_dihedral_restraint.weight',
                 '_nef_dihedral_restraint.target_value', '_nef_dihedral_restraint.target_value_uncertainty',
                 '_nef_dihedral_restraint.lower_linear_limit', '_nef_dihedral_restraint.lower_limit',
                 '_nef_dihedral_restraint.upper_limit', '_nef_dihedral_restraint.upper_linear_limit',
                 '_nef_dihedral_restraint.name']
        outtag = ['_Torsion_angle_constraint.Index_ID', '_Torsion_angle_constraint.ID',
                  '_Torsion_angle_constraint.Combination_ID', '_Torsion_angle_constraint.Auth_asym_ID_1',
                  '_Torsion_angle_constraint.Auth_seq_ID_1', '_Torsion_angle_constraint.Auth_comp_ID_1',
                  '_Torsion_angle_constraint.Auth_atom_ID_1', '_Torsion_angle_constraint.Auth_asym_ID_2',
                  '_Torsion_angle_constraint.Auth_seq_ID_2', '_Torsion_angle_constraint.Auth_comp_ID_2',
                  '_Torsion_angle_constraint.Auth_atom_ID_2', '_Torsion_angle_constraint.Auth_asym_ID_3',
                  '_Torsion_angle_constraint.Auth_seq_ID_3', '_Torsion_angle_constraint.Auth_comp_ID_3',
                  '_Torsion_angle_constraint.Auth_atom_ID_3', '_Torsion_angle_constraint.Auth_asym_ID_4',
                  '_Torsion_angle_constraint.Auth_seq_ID_4', '_Torsion_angle_constraint.Auth_comp_ID_4',
                  '_Torsion_angle_constraint.Auth_atom_ID_4', '_Torsion_angle_constraint.Weight',
                  '_Torsion_angle_constraint.Angle_target_val', '_Torsion_angle_constraint.Angle_target_val_err',
                  '_Torsion_angle_constraint.Angle_lower_linear_limit',
                  '_Torsion_angle_constraint.Angle_lower_bound_val', '_Torsion_angle_constraint.Angle_upper_bound_val',
                  '_Torsion_angle_constraint.Angle_upper_linear_limit', '_Torsion_angle_constraint.Torsion_angle_name',
                  '_Torsion_angle_constraint.Entity_assembly_ID_1', '_Torsion_angle_constraint.Comp_index_ID_1',
                  '_Torsion_angle_constraint.Comp_ID_1', '_Torsion_angle_constraint.Atom_ID_1',
                  '_Torsion_angle_constraint.Entity_assembly_ID_2', '_Torsion_angle_constraint.Comp_index_ID_2',
                  '_Torsion_angle_constraint.Comp_ID_2', '_Torsion_angle_constraint.Atom_ID_2',
                  '_Torsion_angle_constraint.Entity_assembly_ID_3', '_Torsion_angle_constraint.Comp_index_ID_3',
                  '_Torsion_angle_constraint.Comp_ID_3', '_Torsion_angle_constraint.Atom_ID_3',
                  '_Torsion_angle_constraint.Entity_assembly_ID_4', '_Torsion_angle_constraint.Comp_index_ID_4',
                  '_Torsion_angle_constraint.Comp_ID_4', '_Torsion_angle_constraint.Atom_ID_4']
        indat = ['1', '1', '.', 'A', '394', 'ASP', 'C', 'A', '395', 'ARG', 'N', 'A', '395', 'ARG', 'CA', 'A', '395',
                 'ARG', 'C', '1', '.', '.', '.', '-76', '-56', '.', 'PHI']
        outdat = [['1', '1', '.', 'A', '394', 'ASP', 'C', 'A', '395', 'ARG', 'N', 'A', '395', 'ARG', 'CA', 'A', '395',
                   'ARG', 'C', '1', '.', '.', '.', '-76', '-56', '.', 'PHI', 1, 23, 'ASP', 'C', 1, 24, 'ARG', 'N', 1,
                   24, 'ARG', 'CA', 1, 24, 'ARG', 'C']]
        self.assertEqual(bt.translate_row(intag, outtag, indat), outdat)

    def test_translate_seq_row(self):
        bt = NEFT.NEFTranslator()
        bt.cid = [177, 1, 1]
        bt.chains = ['A', 'B', 'C']
        intag = ['_nef_sequence.index', '_nef_sequence.chain_code', '_nef_sequence.sequence_code',
                 '_nef_sequence.residue_name', '_nef_sequence.linking', '_nef_sequence.residue_variant',
                 '_nef_sequence.cis_peptide']
        outtag = ['_Chem_comp_assembly.NEF_index', '_Chem_comp_assembly.Auth_asym_ID',
                  '_Chem_comp_assembly.Auth_seq_ID', '_Chem_comp_assembly.Auth_comp_ID',
                  '_Chem_comp_assembly.Sequence_linking', '_Chem_comp_assembly.Auth_variant_ID',
                  '_Chem_comp_assembly.Cis_residue', '_Chem_comp_assembly.Entity_assembly_ID',
                  '_Chem_comp_assembly.Comp_index_ID', '_Chem_comp_assembly.Comp_ID']
        indat = ['177', 'A', '548', 'SER', 'middle', '.', '.']
        outdat = [['177', 'A', '548', 'SER', 'middle', '.', '.', 1, 177, 'SER']]
        self.assertEqual(bt.translate_seq_row(intag, outtag, indat), outdat)

    def test_translate_restraint_row(self):
        bt = NEFT.NEFTranslator()
        bt.seqDict = {('A', '372'): (1, 1), ('A', '373'): (1, 2), ('A', '374'): (1, 3), ('A', '375'): (1, 4),
                      ('A', '376'): (1, 5), ('A', '377'): (1, 6), ('A', '378'): (1, 7), ('A', '379'): (1, 8),
                      ('A', '380'): (1, 9), ('A', '381'): (1, 10), ('A', '382'): (1, 11), ('A', '383'): (1, 12),
                      ('A', '384'): (1, 13), ('A', '385'): (1, 14), ('A', '386'): (1, 15), ('A', '387'): (1, 16),
                      ('A', '388'): (1, 17), ('A', '389'): (1, 18), ('A', '390'): (1, 19), ('A', '391'): (1, 20),
                      ('A', '392'): (1, 21), ('A', '393'): (1, 22), ('A', '394'): (1, 23), ('A', '395'): (1, 24),
                      ('A', '396'): (1, 25), ('A', '397'): (1, 26), ('A', '398'): (1, 27), ('A', '399'): (1, 28),
                      ('A', '400'): (1, 29), ('A', '401'): (1, 30), ('A', '402'): (1, 31), ('A', '403'): (1, 32),
                      ('A', '404'): (1, 33), ('A', '405'): (1, 34), ('A', '406'): (1, 35), ('A', '407'): (1, 36),
                      ('A', '408'): (1, 37), ('A', '409'): (1, 38), ('A', '410'): (1, 39), ('A', '411'): (1, 40),
                      ('A', '412'): (1, 41), ('A', '413'): (1, 42), ('A', '414'): (1, 43), ('A', '415'): (1, 44),
                      ('A', '416'): (1, 45), ('A', '417'): (1, 46), ('A', '418'): (1, 47), ('A', '419'): (1, 48),
                      ('A', '420'): (1, 49), ('A', '421'): (1, 50), ('A', '422'): (1, 51), ('A', '423'): (1, 52),
                      ('A', '424'): (1, 53), ('A', '425'): (1, 54), ('A', '426'): (1, 55), ('A', '427'): (1, 56),
                      ('A', '428'): (1, 57), ('A', '429'): (1, 58), ('A', '430'): (1, 59), ('A', '431'): (1, 60),
                      ('A', '432'): (1, 61), ('A', '433'): (1, 62), ('A', '434'): (1, 63), ('A', '435'): (1, 64),
                      ('A', '436'): (1, 65), ('A', '437'): (1, 66), ('A', '438'): (1, 67), ('A', '439'): (1, 68),
                      ('A', '440'): (1, 69), ('A', '441'): (1, 70), ('A', '442'): (1, 71), ('A', '443'): (1, 72),
                      ('A', '444'): (1, 73), ('A', '445'): (1, 74), ('A', '446'): (1, 75), ('A', '447'): (1, 76),
                      ('A', '448'): (1, 77), ('A', '449'): (1, 78), ('A', '450'): (1, 79), ('A', '451'): (1, 80),
                      ('A', '452'): (1, 81), ('A', '453'): (1, 82), ('A', '454'): (1, 83), ('A', '455'): (1, 84),
                      ('A', '456'): (1, 85), ('A', '457'): (1, 86), ('A', '458'): (1, 87), ('A', '459'): (1, 88),
                      ('A', '460'): (1, 89), ('A', '461'): (1, 90), ('A', '462'): (1, 91), ('A', '463'): (1, 92),
                      ('A', '464'): (1, 93), ('A', '465'): (1, 94), ('A', '466'): (1, 95), ('A', '467'): (1, 96),
                      ('A', '468'): (1, 97), ('A', '469'): (1, 98), ('A', '470'): (1, 99), ('A', '471'): (1, 100),
                      ('A', '472'): (1, 101), ('A', '473'): (1, 102), ('A', '474'): (1, 103), ('A', '475'): (1, 104),
                      ('A', '476'): (1, 105), ('A', '477'): (1, 106), ('A', '478'): (1, 107), ('A', '479'): (1, 108),
                      ('A', '480'): (1, 109), ('A', '481'): (1, 110), ('A', '482'): (1, 111), ('A', '483'): (1, 112),
                      ('A', '484'): (1, 113), ('A', '485'): (1, 114), ('A', '486'): (1, 115), ('A', '487'): (1, 116),
                      ('A', '488'): (1, 117), ('A', '489'): (1, 118), ('A', '490'): (1, 119), ('A', '491'): (1, 120),
                      ('A', '492'): (1, 121), ('A', '493'): (1, 122), ('A', '494'): (1, 123), ('A', '495'): (1, 124),
                      ('A', '496'): (1, 125), ('A', '497'): (1, 126), ('A', '498'): (1, 127), ('A', '499'): (1, 128),
                      ('A', '500'): (1, 129), ('A', '501'): (1, 130), ('A', '502'): (1, 131), ('A', '503'): (1, 132),
                      ('A', '504'): (1, 133), ('A', '505'): (1, 134), ('A', '506'): (1, 135), ('A', '507'): (1, 136),
                      ('A', '508'): (1, 137), ('A', '509'): (1, 138), ('A', '510'): (1, 139), ('A', '511'): (1, 140),
                      ('A', '512'): (1, 141), ('A', '513'): (1, 142), ('A', '514'): (1, 143), ('A', '515'): (1, 144),
                      ('A', '516'): (1, 145), ('A', '517'): (1, 146), ('A', '518'): (1, 147), ('A', '519'): (1, 148),
                      ('A', '520'): (1, 149), ('A', '521'): (1, 150), ('A', '522'): (1, 151), ('A', '523'): (1, 152),
                      ('A', '524'): (1, 153), ('A', '525'): (1, 154), ('A', '526'): (1, 155), ('A', '527'): (1, 156),
                      ('A', '528'): (1, 157), ('A', '529'): (1, 158), ('A', '530'): (1, 159), ('A', '531'): (1, 160),
                      ('A', '532'): (1, 161), ('A', '533'): (1, 162), ('A', '534'): (1, 163), ('A', '535'): (1, 164),
                      ('A', '536'): (1, 165), ('A', '537'): (1, 166), ('A', '538'): (1, 167), ('A', '539'): (1, 168),
                      ('A', '540'): (1, 169), ('A', '541'): (1, 170), ('A', '542'): (1, 171), ('A', '543'): (1, 172),
                      ('A', '544'): (1, 173), ('A', '545'): (1, 174), ('A', '546'): (1, 175), ('A', '547'): (1, 176),
                      ('A', '548'): (1, 177), ('A', '549'): (1, 178), ('A', '550'): (1, 179), ('A', '551'): (1, 180),
                      ('A', '552'): (1, 181), ('A', '553'): (1, 182), ('A', '554'): (1, 183), ('A', '555'): (1, 184),
                      ('A', '556'): (1, 185), ('A', '557'): (1, 186), ('A', '558'): (1, 187), ('A', '559'): (1, 188),
                      ('A', '560'): (1, 189), ('A', '561'): (1, 190), ('A', '562'): (1, 191), ('A', '563'): (1, 192),
                      ('A', '564'): (1, 193), ('A', '565'): (1, 194), ('A', '566'): (1, 195), ('A', '567'): (1, 196),
                      ('A', '568'): (1, 197), ('A', '569'): (1, 198), ('A', '570'): (1, 199), ('A', '571'): (1, 200),
                      ('A', '572'): (1, 201), ('A', '573'): (1, 202), ('A', '574'): (1, 203), ('A', '575'): (1, 204),
                      ('A', '576'): (1, 205), ('A', '577'): (1, 206), ('A', '578'): (1, 207), ('A', '579'): (1, 208),
                      ('A', '580'): (1, 209), ('A', '581'): (1, 210), ('A', '582'): (1, 211), ('A', '583'): (1, 212),
                      ('A', '584'): (1, 213), ('A', '585'): (1, 214), ('A', '586'): (1, 215), ('B', '1'): (2, 1),
                      ('B', '2'): (2, 2), ('B', '3'): (2, 3), ('B', '4'): (2, 4), ('B', '5'): (2, 5),
                      ('C', '1'): (3, 1), ('C', '2'): (3, 2), ('C', '3'): (3, 3), ('C', '4'): (3, 4),
                      ('C', '5'): (3, 5)}
        intag = ['_nef_distance_restraint.index', '_nef_distance_restraint.restraint_id',
                 '_nef_distance_restraint.restraint_combination_id', '_nef_distance_restraint.chain_code_1',
                 '_nef_distance_restraint.sequence_code_1', '_nef_distance_restraint.residue_name_1',
                 '_nef_distance_restraint.atom_name_1', '_nef_distance_restraint.chain_code_2',
                 '_nef_distance_restraint.sequence_code_2', '_nef_distance_restraint.residue_name_2',
                 '_nef_distance_restraint.atom_name_2', '_nef_distance_restraint.weight',
                 '_nef_distance_restraint.target_value', '_nef_distance_restraint.target_value_uncertainty',
                 '_nef_distance_restraint.lower_linear_limit', '_nef_distance_restraint.lower_limit',
                 '_nef_distance_restraint.upper_limit', '_nef_distance_restraint.upper_linear_limit']
        outtag = ['_Gen_dist_constraint.Index_ID', '_Gen_dist_constraint.ID', '_Gen_dist_constraint.Combination_ID',
                  '_Gen_dist_constraint.Auth_asym_ID_1', '_Gen_dist_constraint.Auth_seq_ID_1',
                  '_Gen_dist_constraint.Auth_comp_ID_1', '_Gen_dist_constraint.Auth_atom_ID_1',
                  '_Gen_dist_constraint.Auth_asym_ID_2', '_Gen_dist_constraint.Auth_seq_ID_2',
                  '_Gen_dist_constraint.Auth_comp_ID_2', '_Gen_dist_constraint.Auth_atom_ID_2',
                  '_Gen_dist_constraint.Weight', '_Gen_dist_constraint.Target_val',
                  '_Gen_dist_constraint.Target_val_uncertainty', '_Gen_dist_constraint.Lower_linear_limit',
                  '_Gen_dist_constraint.Distance_lower_bound_val', '_Gen_dist_constraint.Distance_upper_bound_val',
                  '_Gen_dist_constraint.Upper_linear_limit', '_Gen_dist_constraint.Entity_assembly_ID_1',
                  '_Gen_dist_constraint.Comp_index_ID_1', '_Gen_dist_constraint.Comp_ID_1',
                  '_Gen_dist_constraint.Atom_ID_1', '_Gen_dist_constraint.Entity_assembly_ID_2',
                  '_Gen_dist_constraint.Comp_index_ID_2', '_Gen_dist_constraint.Comp_ID_2',
                  '_Gen_dist_constraint.Atom_ID_2', '_Gen_dist_constraint.Member_logic_code']
        indat = ['549', '389', '.', 'A', '384', 'TYR', 'HD%', 'A', '449', 'CYS', 'HBy', '1', '.', '.', '.', '.',
                 '5.7', '.']
        outdat = [['549', '389', '.', 'A', '384', 'TYR', 'HD%', 'A', '449', 'CYS', 'HBy', '1', '.', '.', '.', '.',
                   '5.7', '.', 1, 13, 'TYR', 'HD1', 1, 78, 'CYS', 'HB3', None],
                  ['549', '389', '.', 'A', '384', 'TYR', 'HD%', 'A', '449', 'CYS', 'HBy', '1', '.', '.', '.', '.',
                   '5.7', '.', 1, 13, 'TYR', 'HD2', 1, 78, 'CYS', 'HB3', None]]
        self.assertEqual(bt.translate_restraint_row(intag, outtag, indat), outdat)

    def test_nef_nmrstar(self):
        bt = NEFT.NEFTranslator()
        bt.nef_to_nmrstar('data/2mqq.nef', star_file='data/test_out.str')
        self.assertTrue(bt.validate_file('data/test_out.str', 'A')[0])
        self.assertTrue(bt.validate_file('data/test_out.str')[0])