
    /*****************************************************************************
    *   Specifies all debug adjustments for the application.
    *
    *   @author     Christopher Stock
    *   @version    0.0.7
    *****************************************************************************/
    class MfgDebugSettings
    {
        /** The debug constant that represents the 'RELEASE'-mode. */
        public      static      MODE_RELEASE                                :number             = 0;
        /** The debug constant that represents the 'DEBUG'-mode. */
        public      static      MODE_DEBUG                                  :number             = 1;

        /** Disables gravity and enables direct vertical movements. */
        public      static      DEBUG_DISABLE_GRAVITY                       :boolean            = true;
        /** Disables all sounds. */
        public      static      DEBUG_DISABLE_SOUNDS                        :boolean            = true;
        /** Hides all image sprites. */
        public      static      DEBUG_DISABLE_SPRITES                       :boolean            = true;

        /** The number of items to show on the level. */
        public      static      DEBUG_ITEM_COUNT                            :number             = 20;

        /** Specifies if animation anchors should be drawn for debug purposes. */
        public      static      DEBUG_DRAW_ANIMATION_ANCHORS                :boolean            = true;
        /** Specifies if a debug rect shall be drawn over the player. */
        public      static      DEBUG_DRAW_RECT_PLAYER                      :boolean            = true;
        /** Specifies if a debug rect shall be drawn over the walls. */
        public      static      DEBUG_DRAW_RECT_WALL                        :boolean            = true;
        /** Specifies if a debug rect shall be drawn over the items. */
        public      static      DEBUG_DRAW_RECT_ITEM                        :boolean            = true;

        /** The width of the stroke for item debug rects. */
        public      static      DEBUG_SIZE_STROKE                           :number             = 1;
        /** The width of the debug collision indicator. */
        public      static      DEBUG_SIZE_COLLISION_INDICATOR              :number             = 10;
        /** The width of the cross for the animation anchor. */
        public      static      DEBUG_SIZE_ANIMATION_ANCHOR                 :number             = 3;

        /** The color for the debug collision indicator. */
        public      static      DEBUG_COLOR_COLLISION_INDICATOR             :string             = LibUI.COLOR_RED_TRANSLUCENT_50;
        /** The color for the debug animation anchors. */
        public      static      DEBUG_COLOR_ANCHORS                         :string             = LibUI.COLOR_WHITE_OPAQUE;

        /** The color for the player's debug rect's border. */
        public      static      DEBUG_COLOR_RECT_PLAYER_BORDER              :string             = LibUI.COLOR_YELLOW_OPAQUE;
        /** The color for the player's debug rect's fill. */
        public      static      DEBUG_COLOR_RECT_PLAYER_FILL                :string             = LibUI.COLOR_GREY_TRANSLUCENT_50;

        /** The color for the wall's debug rect's border. */
        public      static      DEBUG_COLOR_RECT_WALL_RELUCTANT_BORDER      :string             = LibUI.COLOR_BLUE_OPAQUE;
        /** The color for the wall's debug rect's border. */
        public      static      DEBUG_COLOR_RECT_WALL_SOLID_ALL_BORDER      :string             = LibUI.COLOR_RED_OPAQUE;
        /** The color for the wall's debug rect's border. */
        public      static      DEBUG_COLOR_RECT_WALL_SOLID_TOP_BORDER      :string             = LibUI.COLOR_ORANGE_OPAQUE;
        /** The color for the wall's debug rect's fill. */
        public      static      DEBUG_COLOR_RECT_WALL_FILL                  :string             = LibUI.COLOR_GREY_TRANSLUCENT_50;

        /** The color for the item's debug rect's border. */
        public      static      DEBUG_COLOR_RECT_ITEM_BORDER                :string             = LibUI.COLOR_GREEN_OPAQUE;
        /** The color for the item's debug rect's fill. */
        public      static      DEBUG_COLOR_RECT_ITEM_FILL                  :string             = LibUI.COLOR_GREY_TRANSLUCENT_50;
    }