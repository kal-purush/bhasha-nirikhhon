declare module "alloy-editor-core" {

    //
    // CKEDITOR
    //
    // ----------------------------------------------------------------------

    interface CKEDITOR {
    }

    //
    // Attribute
    // ----------------------------------------------------------------------

    interface Attribute {
        get(attr: string): any;
        set(attr: string, value: any): void;
    }

    //
    // Buttons
    // ----------------------------------------------------------------------

    interface ButtonActionStyle {}
    interface ButtonBold {}
    interface ButtonCamera {}
    interface ButtonCameraImage {}
    interface ButtonCode {}
    interface ButtonCommand {}
    interface ButtonCommandListItem {}
    interface ButtonH1 {}
    interface ButtonH2 {}
    interface ButtonHline {}
    interface ButtonImage {}
    interface ButtonImageAlignLeft {}
    interface ButtonImageAlignRight {}
    interface ButtonItalic {}
    interface ButtonLink {}
    interface ButtonOrderedList {}
    interface ButtonParagraphAlignLeft {}
    interface ButtonParagraphAlignRight {}
    interface ButtonParagraphCenter {}
    interface ButtonParagraphJustify {}
    interface ButtonQuote {}
    interface ButtonRemoveFormat {}
    interface ButtonStateClasses {}
    interface ButtonStrike {}
    interface ButtonStyle {}
    interface ButtonStyles {}
    interface ButtonStylesList {}
    interface ButtonStylesListItem {}
    interface ButtonStylesListItemRemove {}
    interface ButtonSubscript {}
    interface ButtonTable {}
    interface ButtonTableCell {}
    interface ButtonTableColumn {}
    interface ButtonTableEdit {}
    interface ButtonTableRemove {}
    interface ButtonTableRow {}
    interface ButtonUnderline {}
    interface ButtonUnorderedlist {}
    interface ButtonsStylesListHeader {}

    //
    // Selections
    // ----------------------------------------------------------------------

    interface SelectionSetPosition {}
    interface SelectionTest {}
    interface Selections {}

    //
    // Toolbar
    // ----------------------------------------------------------------------

    interface ToolbarAdd {}
    interface ToolbarButtons {}
    interface ToolbarStyles {}

    //
    // Widget
    // ----------------------------------------------------------------------

    interface WidgetArrowBox {}
    interface WidgetDropdown {}
    interface WidgetExclusive {}
    interface WidgetFocusManager {}
    interface WidgetInteractionPoint {}
    interface WidgetPosition {}

    //
    // UI
    // ----------------------------------------------------------------------

    interface UI {}

    //
    // Alloy Editor
    // ----------------------------------------------------------------------

    interface AlloyEditor {
        Attribute: Attribute;
        ButtonActionStyle: ButtonActionStyle;
        ButtonBold: ButtonBold;
        ButtonCamera: ButtonCamera;
        ButtonCameraImage: ButtonCameraImage;
        ButtonCode: ButtonCode;
        ButtonCommand: ButtonCommand;
        ButtonCommandListItem: ButtonCommandListItem;
        ButtonH1: ButtonH1;
        ButtonH2: ButtonH2;
        ButtonHline: ButtonHline;
        ButtonImage: ButtonImage;
        ButtonImageAlignLeft: ButtonImageAlignLeft;
        ButtonImageAlignRight: ButtonImageAlignRight;
        ButtonItalic: ButtonItalic;
        ButtonLink: ButtonLink;
        ButtonOrderedList: ButtonOrderedList;
        ButtonParagraphAlignLeft: ButtonParagraphAlignLeft;
        ButtonParagraphAlignRight: ButtonParagraphAlignRight;
        ButtonParagraphCenter: ButtonParagraphCenter;
        ButtonParagraphJustify: ButtonParagraphJustify;
        ButtonQuote: ButtonQuote;
        ButtonRemoveFormat: ButtonRemoveFormat;
        ButtonStateClasses: ButtonStateClasses;
        ButtonStrike: ButtonStrike;
        ButtonStyle: ButtonStyle;
        ButtonStyles: ButtonStyles;
        ButtonStylesList: ButtonStylesList;
        ButtonStylesListItem: ButtonStylesListItem;
        ButtonStylesListItemRemove: ButtonStylesListItemRemove;
        ButtonSubscript: ButtonSubscript;
        ButtonTable: ButtonTable;
        ButtonTableCell: ButtonTableCell;
        ButtonTableColumn: ButtonTableColumn;
        ButtonTableEdit: ButtonTableEdit
        ButtonTableRemove: ButtonTableRemove;
        ButtonTableRow: ButtonTableRow;
        ButtonUnderline: ButtonUnderline;
        ButtonUnorderedlist: ButtonUnorderedlist;
        ButtonsStylesListHeader: ButtonsStylesListHeader;
        SelectionSetpositionPosition: SelectionSetPosition;
        SelectionTest: SelectionTest;
        Selections: Selections;
        ToolbarAdd: ToolbarAdd;
        ToolbarButtons: ToolbarButtons;
        ToolbarStyles: ToolbarStyles;
        WidgetArrowBox: WidgetArrowBox;
        WidgetDropdown: WidgetDropdown;
        WidgetExclusive: WidgetExclusive
        WidgetFocusManager: WidgetFocusManager;
        WidgetInteractionPoint: WidgetInteractionPoint;
        WidgetPosition: WidgetPosition;
        UI: UI;
    }

    //
    // Entriy Point
    // ----------------------------------------------------------------------

    function editable(node: any, config?: any): void;
}