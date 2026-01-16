package org.dungeon.prototype.service.room.ui;

import org.dungeon.prototype.exception.FileLoadingException;
import org.dungeon.prototype.model.room.content.RoomContent;
import org.dungeon.prototype.properties.CallbackType;
import org.springframework.stereotype.Service;
import org.telegram.telegrambots.meta.api.objects.InputFile;

import javax.imageio.ImageIO;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

import static org.dungeon.prototype.util.FileUtil.*;

@Service
public class RoomRenderer {
    private final Map<EnumMap<CallbackType, Boolean>, BufferedImage> backgroundCache = new HashMap<>();

    public InputFile generateRoomImage(long chatId, EnumMap<CallbackType, Boolean> adjacentRoomsMap, RoomContent content) {
        BufferedImage background;
        if (backgroundCache.containsKey(adjacentRoomsMap)) {
            background = backgroundCache.get(adjacentRoomsMap);
        } else {
            background = createRoomBackground(chatId, adjacentRoomsMap);
            backgroundCache.put(adjacentRoomsMap, background);
        }

        BufferedImage result = new BufferedImage(background.getWidth(), background.getHeight(), BufferedImage.TYPE_INT_ARGB);
        BufferedImage roomContent = getRoomContentLayer(chatId, getRoomAsset(content.getRoomType()));
        Graphics2D g = result.createGraphics();

        g.drawImage(background, 0, 0, null);
        g.drawImage(roomContent, 0, 0, null);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            ImageIO.write(result, "png", baos);
        } catch (IOException e) {
            throw new FileLoadingException(chatId, e.getMessage());
        }
        InputStream inputStream = new ByteArrayInputStream(baos.toByteArray());
        return new InputFile(inputStream, "rendered_room.png");
    }


    private BufferedImage createRoomBackground(long chatId, EnumMap<CallbackType, Boolean> adjacentRoomMap) {
        BufferedImage background = getBackgroundLayer(chatId);
        BufferedImage result = new BufferedImage(background.getWidth(), background.getHeight(), BufferedImage.TYPE_INT_ARGB);
        Graphics2D g = result.createGraphics();
        g.drawImage(background, 0,0 , null);
        if (adjacentRoomMap.containsKey(CallbackType.LEFT) && adjacentRoomMap.get(CallbackType.LEFT)) {
            BufferedImage leftDoor = getDoorLayerFragment(chatId, CallbackType.LEFT);
            g.drawImage(leftDoor, 0, 0, null);
        }
        if (adjacentRoomMap.containsKey(CallbackType.FORWARD) && adjacentRoomMap.get(CallbackType.FORWARD)) {
            BufferedImage forwardDoor = getDoorLayerFragment(chatId, CallbackType.FORWARD);
            g.drawImage(forwardDoor, 0, 0, null);
        }
        if (adjacentRoomMap.containsKey(CallbackType.RIGHT) && adjacentRoomMap.get(CallbackType.RIGHT)) {
            BufferedImage rightDoor = getDoorLayerFragment(chatId, CallbackType.RIGHT);
            g.drawImage(rightDoor, 0,0, null);
        }
        g.dispose();
        return result;
    }
}