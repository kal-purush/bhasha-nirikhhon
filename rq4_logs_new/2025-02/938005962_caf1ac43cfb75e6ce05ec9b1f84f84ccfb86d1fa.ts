export interface DataCheckResult {
  data_check_end_time: string;
  data_check_start_time: string;
  data_check_time_taken: number;
  data_mismatch: string[];
  query_hash: {
    source_file: boolean;
    target_file: boolean;
    source_count: string;
    target_count: string;
    source_data: string;
    target_data: string;
  };
  result_counter: {
    pass: number;
    fail: number;
  };
  source_data_count: number;
  table_name: string;
  target_data_count: number;
  test_case_id: string;
  test_case_name: string;
  test_end_time: string;
  test_run_id: string;
  test_start_time: string;
}