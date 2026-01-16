module SmallBasic {

    export class GraphicsWindow {

        /** Gets or sets the Background color of the Graphics Window. */
        public static get BackgroundColor(): string { return null; }
        public static set BackgroundColor(value: string) { }

        /** Gets or sets the brush color to be used to fill shapes drawn on the Graphics Window. */
        public static get BrushColor(): string { return null; }
        public static set BrushColor(value: string) { }

        /** Specifies whether or not the Graphics Window can be resized by the user. */
        public static get CanResize(): boolean { return null; }
        public static set CanResize(value: boolean) { }

        /** Gets or sets whether or not the font to be used when drawing text on the Graphics Window, is bold. */
        public static get FontBold(): boolean { return null; }
        public static set FontBold(value: boolean) { }

        /** Gets or sets whether or not the font to be used when drawing text on the Graphics Window, is italic. */
        public static get FontItalic(): boolean { return null; }
        public static set FontItalic(value: boolean) { }

        /** Gets or sets the Font Name to be used when drawing text on the Graphics Window. */
        public static get FontName(): string { return null; }
        public static set FontName(value: string) { }

        /** Gets or sets the Font Size to be used when drawing text on the Graphics Window. */
        public static get FontSize(): number { return null; }
        public static set FontSize(value: number) { }

        /** Gets or sets the Height of the graphics window. */
        public static get Height(): number { return null; }
        public static set Height(value: number) { }

        /** Gets the last key that was pressed or released. */
        public static get LastKey(): string { return null; }

        /** Gets the last text that was entered on the Graphics Window. */
        public static get LastText(): string { return null; }

        /** Gets or sets the Left Position of the graphics window. */
        public static get Left(): number { return null; }
        public static set Left(value: number) { }

        /** Gets the x-position of the mouse relative to the Graphics Window. */
        public static get MouseX(): number { return null; }

        /** Gets the y-position of the mouse relative to the Graphics Window. */
        public static get MouseY(): number { return null; }

        /** Gets or sets the color of the pen used to draw shapes on the Graphics Window. */
        public static get PenColor(): string { return null; }
        public static set PenColor(value: string) { }

        /** Gets or sets the width of the pen used to draw shapes on the Graphics Window. */
        public static get PenWidth(): number { return null; }
        public static set PenWidth(value: number) { }

        /** Gets or sets the title for the graphics window. */
        public static get Title(): string { return null; }
        public static set Title(value: string) { }

        /** Gets or sets the Top Position of the graphics window. */
        public static get Top(): number { return null; }
        public static set Top(value: number) { }

        /** Gets or sets the Width of the graphics window. */
        public static get Width(): number { return null; }
        public static set Width(value: number) { }

        /** Clears the window. */
        public static Clear() { }

        /**
         * Draws a line of text on the screen at the specified location.
         * @param x - The x co-ordinate of the text start point.
         * @param y - The y co-ordinate of the text start point.
         * @param width - The maximum available width.  This parameter helps define when the text should wrap.
         * @param text - The text to draw.
         */
        public static DrawBoundText(x: number, y: number, width: number, text: string)
        { }

        /**
         * Draws an ellipse on the screen using the selected Pen.
         * @param x - The x co-ordinate of the ellipse.
         * @param y - The y co-ordinate of the ellipse.
         * @param width - The width of the ellipse.
         * @param height - The height of the ellipse.
         */
        public static DrawEllipse(x: number, y: number, width: number, height: number)
        { }

        /**
         * Draws the specified image from memory on to the screen.  
         * @param imageName - The name of the image to draw.
         * @param x - The x co-ordinate of the point to draw the image at.
         * @param y - The y co-ordinate of the point to draw the image at.
         */
        public static DrawImage(imageName: string, x: number, y: number)
        { }

        /** Draws a line from one point to another.
         * @param x1 - The x co-ordinate of the first point.
         * @param y1 - The y co-ordinate of the first point.
         * @param x2 - The x co-ordinate of the second point.
         * @param y2 - The y co-ordinate of the second point.
         */
        public static DrawLine(x1: number, y1: number, x2: number, y2: number)
        { }

        /**
        /// Draws a rectangle on the screen using the selected Pen.
        /// </summary>
        /// <param name="x">
        /// The x co-ordinate of the rectangle.
        /// </param>
        /// <param name="y">
        /// The y co-ordinate of the rectangle.
        /// </param>
        /// <param name="width">
        /// The width of the rectangle.
        /// </param>
        /// <param name="height">
        /// The height of the rectangle.
        /// </param>
        public static static void DrawRectangle(Primitive x, Primitive y, Primitive width, Primitive height)

        /**
        /// Draws the specified image from memory on to the screen, in the specified size.
        /// </summary>
        /// <param name="imageName">
        /// The name of the image to draw
        /// </param>
        /// <param name="x">
        /// The x co-ordinate of the point to draw the image at.
        /// </param>
        /// <param name="y">
        /// The y co-ordinate of the point to draw the image at.
        /// </param>
        /// <param name="width">
        /// The width to draw the image.
        /// </param>
        /// <param name="height">
        /// The height to draw the image.
        /// </param>
        public static static void DrawResizedImage(Primitive imageName, Primitive x, Primitive y, Primitive width, Primitive height)

        /**
        /// Draws a line of text on the screen at the specified location.
        /// </summary>
        /// <param name="x">
        /// The x co-ordinate of the text start point.
        /// </param>
        /// <param name="y">
        /// The y co-ordinate of the text start point.
        /// </param>
        /// <param name="text">
        /// The text to draw
        /// </param>
        public static static void DrawText(Primitive x, Primitive y, Primitive text)

        /**
        /// Draws a triangle on the screen using the selected pen.
        /// </summary>
        /// <param name="x1">
        /// The x co-ordinate of the first point.
        /// </param>
        /// <param name="y1">
        /// The y co-ordinate of the first point.
        /// </param>
        /// <param name="x2">
        /// The x co-ordinate of the second point.
        /// </param>
        /// <param name="y2">
        /// The y co-ordinate of the second point.
        /// </param>
        /// <param name="x3">
        /// The x co-ordinate of the third point.
        /// </param>
        /// <param name="y3">
        /// The y co-ordinate of the third point.
        /// </param>
        public static static void DrawTriangle(Primitive x1, Primitive y1, Primitive x2, Primitive y2, Primitive x3, Primitive y3)

        /**
        /// Fills an ellipse on the screen using the selected Brush.
        /// </summary>
        /// <param name="x">
        /// The x co-ordinate of the ellipse.
        /// </param>
        /// <param name="y">
        /// The y co-ordinate of the ellipse.
        /// </param>
        /// <param name="width">
        /// The width of the ellipse.
        /// </param>
        /// <param name="height">
        /// The height of the ellipse.
        /// </param>
        public static static void FillEllipse(Primitive x, Primitive y, Primitive width, Primitive height)

        /**
        /// Fills a rectangle on the screen using the selected Brush.
        /// </summary>
        /// <param name="x">
        /// The x co-ordinate of the rectangle.
        /// </param>
        /// <param name="y">
        /// The y co-ordinate of the rectangle.
        /// </param>
        /// <param name="width">
        /// The width of the rectangle.
        /// </param>
        /// <param name="height">
        /// The height of the rectangle.
        /// </param>
        public static static void FillRectangle(Primitive x, Primitive y, Primitive width, Primitive height)

        /**
        /// Draws and fills a triangle on the screen using the selected brush.
        /// </summary>
        /// <param name="x1">
        /// The x co-ordinate of the first point.
        /// </param>
        /// <param name="y1">
        /// The y co-ordinate of the first point.
        /// </param>
        /// <param name="x2">
        /// The x co-ordinate of the second point.
        /// </param>
        /// <param name="y2">
        /// The y co-ordinate of the second point.
        /// </param>
        /// <param name="x3">
        /// The x co-ordinate of the third point.
        /// </param>
        /// <param name="y3">
        /// The y co-ordinate of the third point.
        /// </param>
        public static static void FillTriangle(Primitive x1, Primitive y1, Primitive x2, Primitive y2, Primitive x3, Primitive y3)

        /**
        /// Constructs a color given the Red, Green and Blue values.
        /// </summary>
        /// <param name="red">
        /// The red component of the Color (0-255).
        /// </param>
        /// <param name="green">
        /// The green component of the color (0-255).
        /// </param>
        /// <param name="blue">
        /// The blue component of the color (0-255).
        /// </param>
        /// <returns>
        /// Returns a color that can be used to set the brush or pen color.
        /// </returns>
        public static static Primitive GetColorFromRGB(Primitive red, Primitive green, Primitive blue)

        /**
        /// Gets the color of the pixel at the specified x and y co-ordinates.
        /// </summary>
        /// <param name="x">
        /// The x co-ordinate of the pixel.
        /// </param>
        /// <param name="y">
        /// The y co-ordinate of the pixel.
        /// </param>
        /// <returns>
        /// The color of the pixel.
        /// </returns>
        public static static Primitive GetPixel(Primitive x, Primitive y)

        /**
        /// Gets a valid random color.
        /// </summary>
        /// <returns>
        /// A valid random color.
        /// </returns>
        public static static Primitive GetRandomColor()

        /**
        /// Hides the Graphics window.
        /// </summary>
        public static static void Hide()
        */

        /** Draws the pixel specified by the x and y co-ordinates using the specified color.
         * @param x - The x co-ordinate of the pixel.
         * @param y - The y co-ordinate of the pixel.
         * @param color - The color of the pixel to set.
         */
        public static SetPixel(x: number, y: number, color: string)
        {
        }

        /**
        /// Shows the Graphics window to enable interactions with it.
        /// </summary>
        public static static void Show()

        /**
        /// Displays a message box to the user.
        /// </summary>
        /// <param name="text">
        /// The text to be displayed on the message box.
        /// </param>
        /// <param name="title">
        /// The title for the message box.
        /// </param>
        public static static void ShowMessage(Primitive text, Primitive title)

        /**
        /// Raises an event when a key is pressed down on the keyboard.
        /// </summary>
        public static static event SmallBasicCallback KeyDown

        /**
        /// Raises an event when a key is released on the keyboard.
        /// </summary>
        public static static event SmallBasicCallback KeyUp

        /**
        /// Raises an event when the mouse button is clicked down.
        /// </summary>
        public static static event SmallBasicCallback MouseDown

        /**
        /// Raises an event when the mouse is moved around.
        /// </summary>
        public static static event SmallBasicCallback MouseMove

        /**
        /// Raises an event when the mouse button is released.
        /// </summary>
        public static static event SmallBasicCallback MouseUp

        /**
        /// Raises an event when text is entered on the GraphicsWindow.
        /// </summary>
        public static static event SmallBasicCallback TextInput
        */
    }
}

var GraphicsWindow = SmallBasic.GraphicsWindow;