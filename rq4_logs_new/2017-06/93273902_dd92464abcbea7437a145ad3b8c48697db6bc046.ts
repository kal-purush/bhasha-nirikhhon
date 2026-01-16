import 'google-map-react';

declare module 'google-map-react' {

    interface Options {
        fullscreenControl?: boolean;
        fullscreenControlOptions?: FullscreenControlOptions;
    }

    enum ControlPosition {
        BOTTOM_CENTER,
        BOTTOM_LEFT,
        BOTTOM_RIGHT,
        LEFT_BOTTOM,
        LEFT_CENTER,
        LEFT_TOP,
        RIGHT_BOTTOM,
        RIGHT_CENTER,
        RIGHT_TOP,
        TOP_CENTER,
        TOP_LEFT,
        TOP_RIGHT,
    }

    interface PositionOptions {
        position?: ControlPosition;
    }

    type FullscreenControlOptions = PositionOptions;

}