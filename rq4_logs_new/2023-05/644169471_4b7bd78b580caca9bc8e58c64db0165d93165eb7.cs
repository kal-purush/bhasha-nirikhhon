
using System.Drawing.Imaging;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;

namespace RemoteDesktop
{
    internal enum dataFormat { checkConnection = 1, handle };

    internal static class RDFunctions
    {
        internal static Image Capture()
        {
            Rectangle bound = Screen.PrimaryScreen.Bounds;
            Bitmap bitmap = new Bitmap(bound.Width, bound.Height, PixelFormat.Format32bppArgb);
            using (Graphics graphics = Graphics.FromImage(bitmap))
            {
                graphics.CopyFromScreen(bound.X, bound.Y, 0, 0, bound.Size, CopyPixelOperation.SourceCopy);
                CURSORINFO cursorInfo;
                cursorInfo.cbSize = Marshal.SizeOf(typeof(CURSORINFO));
                if (User32.GetCursorInfo(out cursorInfo))
                {
                    if (cursorInfo.flags == User32.CURSOR_SHOWING)
                    {
                        var iconPointer = User32.CopyIcon(cursorInfo.hCursor);
                        ICONINFO iconInfo;
                        int iconX, iconY;
                        if (User32.GetIconInfo(iconPointer, out iconInfo))
                        {
                            iconX = cursorInfo.ptScreenPos.x - ((int)iconInfo.xHotspot);
                            iconY = cursorInfo.ptScreenPos.y - ((int)iconInfo.yHotspot);
                            User32.DrawIcon(graphics.GetHdc(), iconX, iconY, cursorInfo.hCursor);
                            graphics.ReleaseHdc();
                        }
                    }
                }
            }
            return bitmap;
        }

        internal static byte[] CreateBytesSend(byte[] bytesData, dataFormat type)
        {
            int bdlength = bytesData.Length;
            byte[] bytesSend = new byte[bdlength + 7];
            byte[] byteHeader = Encoding.ASCII.GetBytes(((int)type).ToString());
            byte[] bytesLength = Encoding.ASCII.GetBytes(bdlength.ToString());
            Buffer.BlockCopy(byteHeader, 0, bytesSend, 0, 1);
            Buffer.BlockCopy(bytesLength, 0, bytesSend, 1, bytesLength.Length);
            Buffer.BlockCopy(bytesData, 0, bytesSend, 7, bdlength);
            return bytesSend;
        }

        internal static byte[] ReadExactly(NetworkStream stream, int count)
        {
            byte[] buffer = new byte[count];
            int offset = 0;
            while (offset < count)
            {
                int read = stream.Read(buffer, offset, count - offset);
                if (read == 0)
                    throw new System.IO.EndOfStreamException();
                offset += read;
            }
            System.Diagnostics.Debug.Assert(offset == count);
            return buffer;
        }

        internal static byte[] ConvertInputToBytes(Input input)
        {
            int size = Marshal.SizeOf(input);
            byte[] arr = new byte[size];
            IntPtr ptr = IntPtr.Zero;
            try
            {
                ptr = Marshal.AllocHGlobal(size);
                Marshal.StructureToPtr(input, ptr, true);
                Marshal.Copy(ptr, arr, 0, size);
            }
            finally
            {
                Marshal.FreeHGlobal(ptr);
            }
            return arr;
        }

        internal static Input ConvertBytesToInput(byte[] arr)
        {
            Input input = new Input();
            int size = Marshal.SizeOf(input);
            IntPtr ptr = IntPtr.Zero;
            try
            {
                ptr = Marshal.AllocHGlobal(size);
                Marshal.Copy(arr, 0, ptr, size);
                input = (Input)Marshal.PtrToStructure(ptr, input.GetType());
            }
            finally
            {
                Marshal.FreeHGlobal(ptr);
            }
            return input;
        }
    }
}