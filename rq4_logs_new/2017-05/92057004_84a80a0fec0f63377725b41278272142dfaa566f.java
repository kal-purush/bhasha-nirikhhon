package se.perfektum.typeaheadfiles;

import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.win32.StdCallLibrary;
import com.sun.jna.win32.W32APIOptions;

/**
 * Windows specific mapping interface
 * For more information, see the following links:
 * https://msdn.microsoft.com/en-us/library/ms646309%28VS.85%29.aspx
 * https://msdn.microsoft.com/en-us/library/ms684242%28VS.85%29.aspx
 */
public interface User32 extends StdCallLibrary {
    User32 INSTANCE = (User32) Native.loadLibrary("User32", User32.class, W32APIOptions.UNICODE_OPTIONS);

    // dwWakeMask Constants
    int QS_ALLEVENTS = 0x04BF;
    int QS_ALLINPUT = 0x04FF;
    int QS_ALLPOSTMESSAGE = 0x0100;
    int QS_HOTKEY = 0x0080;
    int QS_INPUT = 0x407;
    int QS_KEY = 0x0001;
    int QS_MOUSE = 0x0006;
    int QS_MOUSEBUTTON = 0x0004;
    int QS_MOUSEMOVE = 0x0002;
    int QS_PAINT = 0x0020;
    int QS_POSTMESSAGE = 0x0008;
    int QS_RAWINPUT = 0x0400;
    int QS_SENDMESSAGE = 0x0040;
    int QS_TIMER = 0x0010;

    int INFINITE = 0xFFFFFFFF;

    /*
    DWORD WINAPI MsgWaitForMultipleObjects(
    __in  DWORD nCount,
    __in  const HANDLE *pHandles,
    __in  BOOL bWaitAll,
    __in  DWORD dwMilliseconds,
    __in  DWORD dwWakeMask
    );*/
    int MsgWaitForMultipleObjects(int nCount, Pointer pHandles, boolean bWaitAll, int dwMilliSeconds, int dwWakeMask);

    // fsModifiers vaues
    int MOD_ALT = 0x0001;
    int MOD_CONTROL = 0x0002;
    int MOD_NOREPEAT = 0x4000;
    int MOD_SHIFT = 0x0004;
    int MOD_WIN = 0x0008;

    /*
    BOOL WINAPI RegisterHotKey(
    __in_opt  HWND hWnd,
    __in      int id,
    __in      UINT fsModifiers,
    __in      UINT vk
    );
    */
    boolean RegisterHotKey(Pointer hWnd, int id, int fsModifiers, int vk);

    /*
    BOOL WINAPI GetMessage(
    _Out_    LPMSG lpMsg,
    _In_opt_ HWND  hWnd,
    _In_     UINT  wMsgFilterMin,
    _In_     UINT  wMsgFilterMax
    );
    */
    boolean GetMessage(Pointer lpMsg, Pointer hWnd, int wMsgFilterMin, int wMsgFilterMax);


}