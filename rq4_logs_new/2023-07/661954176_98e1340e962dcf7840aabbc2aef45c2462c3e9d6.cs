using System.Runtime.InteropServices;

namespace LibuvSharp;

public static class Libuv
{
    public const string Lib = "libuv";

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_listen(IntPtr stream, int backlog, callback callback);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_accept(IntPtr server, IntPtr client);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_tcp_getsockname(IntPtr handle, IntPtr addr, ref int length);


    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_pipe_init(IntPtr loop, IntPtr handle, int ipc);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_pipe_bind(IntPtr handle, string name);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_pipe_getsockname(IntPtr handle, IntPtr buf, ref IntPtr len);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_async_send(IntPtr handle);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_async_init(IntPtr loop, IntPtr handle, uv_handle_cb callback);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_check_init(IntPtr loop, IntPtr idle);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_check_start(IntPtr check, uv_handle_cb callback);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_check_stop(IntPtr check);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern uint uv_version();

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_ip4_addr(string ip, int port, out sockaddr_in address);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_ip6_addr(string ip, int port, out sockaddr_in6 address);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_ip4_name(IntPtr src, byte[] dst, IntPtr size);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_ip6_name(IntPtr src, byte[] dst, IntPtr size);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_req_size(RequestType type);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern unsafe sbyte* uv_version_string();

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void uv_loadavg(IntPtr avg);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_cpu_info(out IntPtr info, out int count);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void uv_free_cpu_info(IntPtr info, int count);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_interface_addresses(out IntPtr address, out int count);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void uv_free_interface_addresses(IntPtr address, int count);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern long uv_get_total_memory();

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern long uv_get_free_memory();

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern ulong uv_hrtime();

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_uptime(out double uptime);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_dlopen(IntPtr name, IntPtr handle);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_dlopen(string name, IntPtr handle);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void uv_dlclose(IntPtr handle);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_dlsym(IntPtr handle, string name, out IntPtr ptr);

    [DllImport(Libuv.Lib)]
    internal static extern IntPtr uv_dlerror(IntPtr handle);

    [DllImport(Libuv.Lib)]
    internal static extern IntPtr uv_dlerror_free(IntPtr handle);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern unsafe sbyte* uv_strerror(int systemErrorCode);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern unsafe sbyte* uv_err_name(int systemErrorCode);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_fs_event_init(IntPtr loop, IntPtr handle);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_fs_event_start(IntPtr handle, uv_fs_event_cb callback, string filename, int flags);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_fs_event_getpath(IntPtr handle, IntPtr buf, ref IntPtr len);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_fs_event_stop(IntPtr handle);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_is_active(IntPtr handle);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_is_closing(IntPtr handle);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void uv_ref(IntPtr handle);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void uv_unref(IntPtr handle);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_has_ref(IntPtr handle);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void uv_close(IntPtr handle, close_callback cb);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_tcp_init(IntPtr loop, IntPtr handle);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_tcp_bind(IntPtr handle, ref sockaddr_in sockaddr, uint flags);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_tcp_bind(IntPtr handle, ref sockaddr_in6 sockaddr, uint flags);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_tcp_simultaneos_accepts(IntPtr handle, int enable);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_tcp_connect(IntPtr req, IntPtr handle, ref sockaddr_in addr, callback callback);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_tcp_connect(IntPtr req, IntPtr handle, ref sockaddr_in6 addr, callback callback);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_tcp_nodelay(IntPtr handle, int enable);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_tcp_keepalive(IntPtr handle, int enable, int delay);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_tcp_getpeername(IntPtr handle, IntPtr addr, ref int length);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_udp_send(IntPtr req, IntPtr handle, uv_buf_t[] bufs, int nbufs, ref sockaddr_in addr,
        callback callback);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_udp_send(IntPtr req, IntPtr handle, uv_buf_t[] bufs, int nbufs, ref sockaddr_in6 addr,
        callback callback);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_udp_init(IntPtr loop, IntPtr handle);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_udp_bind(IntPtr handle, ref sockaddr_in sockaddr, uint flags);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_udp_bind(IntPtr handle, ref sockaddr_in6 sockaddr, uint flags);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_udp_recv_start(IntPtr handle, alloc_callback alloc_callback,
        recv_start_callback callback);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_handle_size(HandleType type);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern HandleType uv_guess_handle(int fd);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_pipe_getpeername(IntPtr handle, IntPtr buf, ref IntPtr len);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void uv_pipe_connect(IntPtr req, IntPtr handle, string name, callback connect_cb);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_write2(IntPtr req, IntPtr handle, uv_buf_t[] bufs, int bufcnt, IntPtr sendHandle,
        callback callback);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_pipe_pending_count(IntPtr handle);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern HandleType uv_pipe_pending_type(IntPtr pipe);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_fs_rmdir(IntPtr loop, IntPtr req, string path, uv_fs_cb callback);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_fs_rename(IntPtr loop, IntPtr req, string path, string newPath, uv_fs_cb callback);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_fs_scandir_next(IntPtr req, out uv_dirent_t ent);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_fs_scandir(IntPtr loop, IntPtr req, string path, int flags, uv_fs_cb callback);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_fs_mkdir(IntPtr loop, IntPtr req, string path, int mode, uv_fs_cb callback);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_fs_open(IntPtr loop, IntPtr req, string path, int flags, int mode, uv_fs_cb callback);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_fs_close(IntPtr loop, IntPtr req, int fd, uv_fs_cb callback);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_fs_read(IntPtr loop, IntPtr req, int fd, uv_buf_t[] buf, int nbufs, long offset,
        uv_fs_cb callback);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_fs_write(IntPtr loop, IntPtr req, int fd, uv_buf_t[] bufs, int nbufs, long offset,
        uv_fs_cb fs_cb);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_fs_stat(IntPtr loop, IntPtr req, string path, uv_fs_cb callback);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_fs_fstat(IntPtr loop, IntPtr req, int fd, uv_fs_cb callback);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_fs_fsync(IntPtr loop, IntPtr req, int fd, uv_fs_cb callback);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_fs_fdatasync(IntPtr loop, IntPtr req, int fd, uv_fs_cb callback);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_fs_ftruncate(IntPtr loop, IntPtr req, int file, long offset, uv_fs_cb callback);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_fs_fchmod(IntPtr loop, IntPtr req, int fd, int mode, uv_fs_cb callback);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_fs_chmod(IntPtr loop, IntPtr req, string path, int mode, uv_fs_cb callback);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_fs_chown(IntPtr loop, IntPtr req, string path, int uid, int gid, uv_fs_cb callback);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_fs_fchown(IntPtr loop, IntPtr req, int fd, int uid, int gid, uv_fs_cb callback);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_fs_unlink(IntPtr loop, IntPtr req, string path, uv_fs_cb callback);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_fs_link(IntPtr loop, IntPtr req, string path, string newPath, uv_fs_cb callback);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_fs_symlink(IntPtr loop, IntPtr req, string path, string newPath, int flags, uv_fs_cb callback);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_fs_readlink(IntPtr loop, IntPtr req, string path, uv_fs_cb callback);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_send_buffer_size(IntPtr handle, out int value);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_recv_buffer_size(IntPtr handle, out int value);
    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_idle_init(IntPtr loop, IntPtr idle);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_idle_start(IntPtr idle, uv_handle_cb callback);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_idle_stop(IntPtr idle);
    
    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr uv_default_loop();

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_loop_init(IntPtr handle);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_loop_close(IntPtr ptr);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr uv_loop_size();

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void uv_run(IntPtr loop, uv_run_mode mode);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void uv_update_time(IntPtr loop);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern ulong uv_now(IntPtr loop);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void uv_walk(IntPtr loop, walk_cb cb, IntPtr arg);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void uv_stop(IntPtr loop);
    
    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_loop_alive(IntPtr loop);
    
    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_backend_timeout(IntPtr loop);
    
    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_backend_fd(IntPtr loop);
    
    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_poll_init(IntPtr loop, IntPtr handle, int fd);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_poll_start(IntPtr handle, int events, poll_callback callback);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_poll_stop(IntPtr handle);
    
    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_prepare_init(IntPtr loop, IntPtr prepare);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_prepare_start(IntPtr prepare, uv_handle_cb callback);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_prepare_stop(IntPtr prepare);
    
    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_get_process_title(IntPtr buffer, IntPtr size);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_set_process_title(string title);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_spawn(IntPtr loop, IntPtr handle, ref uv_process_options_t options);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_exepath(IntPtr buffer, ref IntPtr size);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_process_kill(IntPtr handle, int signum);
    
    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void uv_disable_stdio_inheritance();

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_udp_recv_stop(IntPtr handle);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_udp_set_ttl(IntPtr handle, int ttl);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_udp_set_broadcast(IntPtr handle, int on);
    
    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_udp_set_multicast_ttl(IntPtr handle, int ttl);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_udp_set_multicast_loop(IntPtr handle, int on);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_udp_getsockname(IntPtr handle, IntPtr addr, ref int length);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_udp_try_send(IntPtr handle, uv_buf_t[] bufs, int nbufs, ref sockaddr_in addr);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_udp_try_send(IntPtr handle, uv_buf_t[] bufs, int nbufs, ref sockaddr_in6 addr);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_read_start(IntPtr stream, alloc_callback alloc_callback, read_callback rcallback);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_read_watcher_start(IntPtr stream, Action<IntPtr> read_watcher_callback);

    [DllImport (Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_read_stop(IntPtr stream);

    [DllImport(Libuv.Lib, EntryPoint = "uv_write", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_write(IntPtr req, IntPtr handle, uv_buf_t[] bufs, int bufcnt, callback callback);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_shutdown(IntPtr req, IntPtr handle, callback callback);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_is_readable(IntPtr handle);
    
    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_is_writable(IntPtr handle);
    
    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_try_write(IntPtr handle, uv_buf_t[] bufs, int nbufs);
    
    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_timer_init(IntPtr loop, IntPtr timer);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_timer_start(IntPtr timer, uv_timer_cb callback, ulong timeout, ulong repeat);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_timer_stop(IntPtr timer);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int uv_timer_again(IntPtr timer);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void uv_timer_set_repeat(IntPtr timer, ulong repeat);

    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern ulong uv_timer_get_repeat(IntPtr timer);
    [DllImport(Libuv.Lib, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void uv_fs_req_cleanup(IntPtr req);

    
    
    
    [DllImport("__Internal", EntryPoint = "ntohs", CallingConvention = CallingConvention.Cdecl)]
    internal static extern ushort ntohs_unix(ushort bytes);

    [DllImport("Ws2_32", EntryPoint = "ntohs")]
    internal static extern ushort ntohs_win(ushort bytes);

    [DllImport("kernel32.dll", EntryPoint = "FreeLibrary", SetLastError = true)]
    public static extern bool FreeLibrary(IntPtr hModule);

    [DllImport("kernel32.dll", EntryPoint = "LoadLibraryExW", CharSet = CharSet.Unicode, SetLastError = true)]
    public static extern IntPtr LoadLibraryEx(
        [MarshalAs(UnmanagedType.LPWStr)] string lpFileName,
        IntPtr hFile,
        [MarshalAs(UnmanagedType.U4)] LoadLibraryFlags dwFlags);

    [DllImport("kernel32.dll", EntryPoint = "LoadLibraryExW", CharSet = CharSet.Unicode, SetLastError = true)]
    public static extern IntPtr LoadLibraryEx(
        IntPtr lpFileName,
        IntPtr hFile,
        [MarshalAs(UnmanagedType.U4)] LoadLibraryFlags dwFlags);

    [DllImport("kernel32.dll", CharSet = CharSet.Ansi, EntryPoint = "GetProcAddress", ExactSpelling = true,
        SetLastError = true)]
    public static extern IntPtr GetProcAddress(IntPtr hModule, string lpProcName);
}

[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
delegate void uv_timer_cb(IntPtr loop);

[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
internal delegate void read_callback(IntPtr stream, IntPtr size, uv_buf_t buf);

[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
internal delegate void uv_fs_cb(IntPtr intPtr);

[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
delegate void uv_fs_event_cb(IntPtr handle, string filename, int events, int status);

[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
internal delegate void callback(IntPtr req, int status);

[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
internal delegate void alloc_callback(IntPtr data, int size, out uv_buf_t buf);

[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
internal delegate void close_callback(IntPtr handle);

[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
internal delegate void recv_start_callback(IntPtr handle, IntPtr nread, ref uv_buf_t buf, IntPtr sockaddr, ushort flags);

[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
internal delegate int buffer_size_function(IntPtr handle, out int value);

[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
public delegate void uv_handle_cb(IntPtr handle);

[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
internal delegate void walk_cb(IntPtr handle, IntPtr arg);

internal delegate void poll_callback(IntPtr handle, int status, int events);


[Flags]
public enum LoadLibraryFlags : uint
{
    DONT_RESOLVE_DLL_REFERENCES = 0x00000001,
    LOAD_IGNORE_CODE_AUTHZ_LEVEL = 0x00000010,
    LOAD_LIBRARY_AS_DATAFILE = 0x00000002,
    LOAD_WITH_ALTERED_SEARCH_PATH = 0x00000008,
}