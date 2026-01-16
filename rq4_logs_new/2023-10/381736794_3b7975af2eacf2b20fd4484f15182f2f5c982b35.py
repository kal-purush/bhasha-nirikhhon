from ctypes import *
from ctypes.wintypes import *

# Windows API-bindings.
user32   = windll.user32
kernel32 = windll.kernel32
gdi32    = windll.gdi32

WNDPROCTYPE = WINFUNCTYPE(c_int, HWND, c_uint, WPARAM, LPARAM)

# Custom return codes.
ERR_CLASS_REGISTRATION = 1
ERR_WINDOW_CREATION = 2

STARTF_USESHOWWINDOW = 0x00000001

# Window Styles.
WS_EX_APPWINDOW = 0x40000
WS_OVERLAPPEDWINDOW = 0xcf0000
WS_CAPTION = 0xc00000
WS_SYSMENU = 0x00080000
WS_VISIBLE = 0x10000000
WS_POPUP = 0x80000000

# Class Styles.
CS_HREDRAW = 2
CS_VREDRAW = 1

# Window color.
WHITE_BRUSH = 0
COLOR_WINDOW = 5
TRANSPARENT = 1

# Show Window values.
SW_SHOWNORMAL = 1
SW_SHOW = 5
SW_SHOWDEFAULT = 10

# Window settings.
CW_USEDEFAULT = 0x80000000

# Window Messages.
WM_CREATE = 1
WM_DESTROY = 2
WM_PAINT = 15
WM_QUIT = 18

# DrawText flags.
DT_VCENTER = 4
DT_CENTER = 1
DT_WORDBREAK = 16
DT_SINGLELINE = 32

# Thread execution States.
ES_CONTINUOUS = 0x80000000
ES_AWAYMODE_REQUIRED = 0x00000040
ES_DISPLAY_REQUIRED = 0x00000002
ES_SYSTEM_REQUIRED = 0x00000001

# System wide Icon and Cursor.
IDI_APPLICATION = user32.LoadIconW(None, 32512) # IDI_APPLICATION
IDC_ARROW = user32.LoadCursorW(None, 32512) # IDC_ARROW

CLIENT_TEXT = create_unicode_buffer('Close to stop.')

# Startup info structure.
class STARTUPINFOW(Structure):
    _fields_ = [("cb", c_uint),
                ("lpReserved", LPWSTR),
                ("lpDesktop", LPWSTR),
                ("lpTitle", LPWSTR),
                ("dwX", c_uint),
                ("dwY", c_uint),
                ("dwXSize", c_uint),
                ("dwYSize", c_uint),
                ("dwXCountChars", c_uint),
                ("dwYCountChars", c_uint),
                ("dwFillAttribute", c_uint),
                ("dwFlags", c_uint),
                ("wShowWindow", c_ushort),
                ("cbReserved2", c_ushort),
                ("lpReserved2", c_char_p),
                ("hStdInput", HANDLE),
                ("hStdOutput", HANDLE),
                ("hStdError", HANDLE)]

# Window class structure.
class WNDCLASSEX(Structure):
    _fields_ = [("cbSize", c_uint),
                ("style", c_uint),
                ("lpfnWndProc", WNDPROCTYPE),
                ("cbClsExtra", c_int),
                ("cbWndExtra", c_int),
                ("hInstance", HINSTANCE),
                ("hIcon", HICON),
                ("hCursor", HANDLE),
                ("hBrush", HBRUSH),
                ("lpszMenuName", LPCWSTR),
                ("lpszClassName", LPCWSTR),
                ("hIconSm", HICON)]

# Paintstruct struct.
class PAINTSTRUCT(Structure):
    _fields_ = [('hdc', HDC),
                ('fErase', BOOL),
                ('rcPaint', BOOL),
                ('fRestore', BOOL),
                ('fIncUpdate', BOOL),
                ('rgbReserved', c_char * 32)]

# Window procedure.
def main_loop(hwnd, msg, w_param, l_param):

    # On creation - returns 0.
    if msg == WM_CREATE:
        kernel32.SetThreadExecutionState(ES_CONTINUOUS | ES_DISPLAY_REQUIRED | ES_SYSTEM_REQUIRED)
        print('SetThreadExecutionState(ES_CONTINUOUS | ES_DISPLAY_REQUIRED | ES_SYSTEM_REQUIRED)')
        return 0

    # On closed - returns 0.
    if msg == WM_DESTROY:
        kernel32.SetThreadExecutionState(ES_CONTINUOUS)
        print('SetThreadExecutionState(ES_CONTINUOUS)')
        user32.PostQuitMessage(0)
        return 0

    # When painting - returns 0.
    if msg == WM_PAINT:
        rc = RECT()
        ps = PAINTSTRUCT()

        user32.GetClientRect(hwnd, byref(rc))
        hdc = user32.BeginPaint(hwnd, byref(ps))

        gdi32.SetBkMode(hdc, TRANSPARENT)
        user32.DrawTextW(hdc, CLIENT_TEXT, -1, byref(rc), DT_SINGLELINE | DT_VCENTER | DT_CENTER | DT_WORDBREAK)

        user32.EndPaint(hwnd, byref(ps))
        return 0
    else:
        # Default everything else.
        return user32.DefWindowProcW(hwnd, msg, w_param, l_param)

# Creates the window and starts the message loop.
def create_window():
    user32.DefWindowProcW.argtypes = (HWND, c_uint, WPARAM, LPARAM)

    # Variables used for window creation.
    hinst = kernel32.GetModuleHandleW(0)

    # Constructing the Window Class and registering it.
    wc = WNDCLASSEX()
    wc.cbSize = sizeof(WNDCLASSEX)
    wc.style = CS_HREDRAW | CS_VREDRAW
    wc.lpfnWndProc = WNDPROCTYPE(main_loop)
    wc.hInstance = hinst
    wc.hIcon = IDI_APPLICATION
    wc.hCursor = IDC_ARROW
    wc.hBrush = COLOR_WINDOW + 1
    wc.lpszClassName = 'KeepRunningPyW'
    wc.hIconSm = IDI_APPLICATION

    if user32.RegisterClassExW(byref(wc)) == 0:
        print('Failed to register class')
        return ERR_CLASS_REGISTRATION

    # Gets the startup info (Show minimized, Maximized and so forth).
    si = STARTUPINFOW( cb = sizeof(STARTUPINFOW) )
    kernel32.GetStartupInfoW(byref(si))
    show_window = si.wShowWindow if si.dwFlags & STARTF_USESHOWWINDOW == STARTF_USESHOWWINDOW else SW_SHOWDEFAULT

    # Creating the Window.
    hwnd = user32.CreateWindowExW(
        0, wc.lpszClassName, 'Keep Running',
        WS_POPUP | WS_CAPTION | WS_SYSMENU,
        CW_USEDEFAULT, 0,
        320, 120, 0, 0, hinst, 0)

    if not hwnd:
        print('Failed to create window')
        return ERR_WINDOW_CREATION

    user32.ShowWindow(hwnd, show_window)

    # Begin the message loop.
    msg = MSG()
    lpmsg = pointer(msg)
    while user32.GetMessageW(lpmsg, 0, 0, 0) != 0:
        if msg.message == WM_QUIT:
            break

        user32.TranslateMessage(lpmsg)
        user32.DispatchMessageW(lpmsg)

    # Returns the last messages wParam (should be zero in normal use).
    return msg.wParam


# If running as a script.
if __name__ == '__main__':

    # Uses the return value as an exit code.
    exit(create_window())