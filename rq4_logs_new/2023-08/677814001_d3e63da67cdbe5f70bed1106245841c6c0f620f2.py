# Imports
from dataclasses import dataclass, field

@dataclass
# holds statistics for a segment
class segment_stats:
    average: str = ''
    stdev: str = ''
    decent_rate: str = ''
    finished_rate: str = ''
    median: str = ''
    improvement_over_time: str = ''

@dataclass
# holds the segment time, id, and date/time that the associated run was started
class segment_time:
    time: str = ''
    id: str = ''
    run_date: str = ''
    run_time: str = ''

@dataclass
# holds basic info for each segment, including detailed stats on best/worst segments
class segment_data:
    name: str = ''
    split_time_pb: str = ''
    segment_pb: str = ''
    segment_gold: segment_time = field(default_factory=segment_time)
    segment_worst: segment_time = field(default_factory=segment_time)
    segment_history: dict = field(default_factory=dict)
    stats: segment_stats = field(default_factory=segment_stats)
    possible_time_save: str = ''
        
@dataclass
class lss:
    game_name: str = ''
    category_name: str = ''
    layout_path: str = ''
    timer_offset: str = ''
    
    # attempts and completed runs
    runs_started: int = 0
    runs_finished: int = 0
    
    # completion percentage
    completed: float = 0.0
    finished_run_times: dict = field(default_factory=dict)
    
    # list of segments
    segments: list = field(default_factory=list)
    
    # sum of best, total playtime
    sob: str = ''
    total_runtime: str = ''
    total_playtime: str = ''
    