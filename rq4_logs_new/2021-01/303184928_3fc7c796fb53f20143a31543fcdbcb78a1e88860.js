import React, { useCallback } from 'react';
import { Graphics, Text } from '@inlet/react-pixi';

const PopulatedCorners = ({ 
    cornerCoordinates,
}) => {
    const drawCornerTargets = useCallback((g) => {
        g.beginFill(0xFF2342);
        g.drawRect(cornerCoordinates.x - 7, cornerCoordinates.y - 7, 14, 14);
        g.endFill();
    }, [cornerCoordinates]);

    return (
        <Graphics draw={drawCornerTargets}>
            <Text
                text="S"
                anchor={0.5}
                style={{fontSize: 12, fill: 'white', fontFamily: 'Arial'}}
                x={cornerCoordinates.x}
                y={cornerCoordinates.y}
            />
        </Graphics>
    );
};

export default PopulatedCorners;