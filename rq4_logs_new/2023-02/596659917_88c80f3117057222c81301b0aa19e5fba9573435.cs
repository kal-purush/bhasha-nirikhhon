using System;
using System.Runtime.InteropServices;

namespace SunSharp.Unity.Editor
{
    internal static class DllImport
    {
        internal const string LibraryName = "editor_sunvox";

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_audio_callback(IntPtr buf, int frames, int latency, uint out_time);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_audio_callback2(IntPtr buf, int frames, int latency, uint out_time, int in_type, int in_channels, IntPtr in_buf);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_close_slot(int slot);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_connect_module(int slot, int source, int destination);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_deinit();

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_disconnect_module(int slot, int source, int destination);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_end_of_song(int slot);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_find_module(int slot, IntPtr name);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_find_pattern(int slot, IntPtr name);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_get_autostop(int slot);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_get_current_line(int slot);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_get_current_line2(int slot);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_get_current_signal_level(int slot, int channel);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern IntPtr sv_get_log(int size);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_get_module_color(int slot, int mod_num);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_get_module_ctl_group(int slot, int mod_num, int ctl_num);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_get_module_ctl_max(int slot, int mod_num, int ctl_num, int scaled);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_get_module_ctl_min(int slot, int mod_num, int ctl_num, int scaled);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern IntPtr sv_get_module_ctl_name(int slot, int mod_num, int ctl_num);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_get_module_ctl_offset(int slot, int mod_num, int ctl_num);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_get_module_ctl_type(int slot, int mod_num, int ctl_num);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_get_module_ctl_value(int slot, int mod_num, int ctl_num, int scaled);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern uint sv_get_module_finetune(int slot, int mod_num);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern uint sv_get_module_flags(int slot, int mod_num);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern IntPtr sv_get_module_inputs(int slot, int mod_num);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern IntPtr sv_get_module_name(int slot, int mod_num);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern IntPtr sv_get_module_outputs(int slot, int mod_num);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern uint sv_get_module_scope2(int slot, int mod_num, int channel, IntPtr dest_buf, uint samples_to_read);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern IntPtr sv_get_module_type(int slot, int mod_num);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern uint sv_get_module_xy(int slot, int mod_num);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_get_number_of_modules(int slot);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_get_number_of_module_ctls(int slot, int mod_num);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_get_number_of_patterns(int slot);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern IntPtr sv_get_pattern_data(int slot, int pat_num);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_get_pattern_event(int slot, int pat_num, int track, int line, int column);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_get_pattern_lines(int slot, int pat_num);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern IntPtr sv_get_pattern_name(int slot, int pat_num);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_get_pattern_tracks(int slot, int pat_num);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_get_pattern_x(int slot, int pat_num);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_get_pattern_y(int slot, int pat_num);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_get_sample_rate();

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_get_song_bpm(int slot);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern uint sv_get_song_length_frames(int slot);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern uint sv_get_song_length_lines(int slot);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern IntPtr sv_get_song_name(int slot);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_get_song_tpl(int slot);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern uint sv_get_ticks();

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern uint sv_get_ticks_per_second();

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_get_time_map(int slot, int start_line, int len, IntPtr dest, int flags);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_init(IntPtr config, int freq, int channels, uint flags);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_load(int slot, IntPtr name);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_load_from_memory(int slot, IntPtr data, uint data_size);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_load_module(int slot, IntPtr file_name, int x, int y, int z);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_load_module_from_memory(int slot, IntPtr data, uint data_size, int x, int y, int z);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_lock_slot(int slot);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_metamodule_load(int slot, int mod_num, IntPtr file_name);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_metamodule_load_from_memory(int slot, int mod_num, IntPtr data, uint data_size);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_module_curve(int slot, int mod_num, int curve_num, IntPtr data, int len, int w);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_new_module(int slot, IntPtr type, IntPtr name, int x, int y, int z);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_new_pattern(int slot, int clone, int x, int y, int tracks, int lines, int icon_seed, IntPtr name);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_open_slot(int slot);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_pattern_mute(int slot, int pat_num, int mute);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_pause(int slot);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_play(int slot);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_play_from_beginning(int slot);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_remove_module(int slot, int mod_num);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_remove_pattern(int slot, int pat_num);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_resume(int slot);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_rewind(int slot, int line_num);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_sampler_load(int slot, int sampler_module, IntPtr file_name, int sample_slot);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_sampler_load_from_memory(int slot, int sampler_module, IntPtr data, uint data_size, int sample_slot);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_save(int slot, IntPtr name);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_send_event(int slot, int track_num, int note, int vel, int module, int ctl, int ctl_val);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_set_autostop(int slot, int autostop);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_set_event_t(int slot, int set, int t);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_set_module_color(int slot, int mod_num, int color);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_set_module_ctl_value(int slot, int mod_num, int ctl_num, int val, int scaled);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_set_module_finetune(int slot, int mod_num, int finetune);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_set_module_name(int slot, int mod_num, IntPtr name);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_set_module_relnote(int slot, int mod_num, int relative_note);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_set_module_xy(int slot, int mod_num, int x, int y);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_set_pattern_event(int slot, int pat_num, int track, int line, int nn, int vv, int mm, int ccee, int xxyy);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_set_pattern_name(int slot, int pat_num, IntPtr name);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_set_pattern_size(int slot, int pat_num, int tracks, int lines);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_set_pattern_xy(int slot, int pat_num, int x, int y);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_set_song_name(int slot, IntPtr name);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_stop(int slot);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_sync_resume(int slot);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_unlock_slot(int slot);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_update_input();

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_volume(int slot, int vol);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_vplayer_load(int slot, int mod_num, IntPtr file_name);

        [DllImport(LibraryName, CharSet = CharSet.Ansi)]
        public static extern int sv_vplayer_load_from_memory(int slot, int mod_num, IntPtr data, uint data_size);
    }
}