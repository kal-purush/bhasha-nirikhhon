import unittest
import pandas.util.testing as pd_test
import os
import shutil

import adr_dhis2_pivot_table_etl as pivot_etl
import pandas as pd

class PivotTableETLGoldenMaster(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        # copy pickles from test resources into the output dir
        pickles_dir = 'resources/pivot_table/pickles'
        dest_dir = 'output/build'
        os.makedirs(dest_dir, exist_ok=True)
        for file_name in os.listdir(pickles_dir):
            pickle_path = os.path.join(pickles_dir, file_name)
            if os.path.isfile(pickle_path):
                pickle_dest = os.path.join(dest_dir, file_name)
                shutil.copy(pickle_path, pickle_dest)

        pivot_etl.AREA_ID_MAP = ''
        pivot_etl.OUTPUT_DIR_NAME = 'output'
        pivot_etl.get_metadata(from_pickle=True)
        pivot_etl.PROGRAM_DATA_COLUMN_CONFIG='resources/pivot_table/anc_column_config.json'
        pivot_etl.PROGRAM_DATA_CATEGORY_CONFIG = 'resources/pivot_table/anc_category_config.json'

    def test_play_anc_pull(self):
        pivot_table_id = 'wIpu9GVn5gG'
        input_df = pivot_etl.get_dhis2_pivot_table_data(pivot_table_id, from_pickle=True)
        # interim csv output helps in comparing types&values vs expected csv file
        pivot_etl.run_pipeline(input_df).to_csv('output/build/actual.csv', index=False)
        actual = pd.read_csv('output/build/actual.csv')

        expected = pd.read_csv('resources/pivot_table/play_dhis2_pull_anc.csv')

        pd_test.assert_frame_equal(expected, actual, check_dtype=False)

if __name__ == '__main__':
    unittest.main()