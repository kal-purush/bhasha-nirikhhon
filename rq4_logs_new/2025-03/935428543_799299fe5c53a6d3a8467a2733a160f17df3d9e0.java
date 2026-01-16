package gui;

import log.Logger;

import javax.swing.*;
import java.awt.event.KeyEvent;

public class MenuBarBuilder {
    public static JMenuBar createMenuBar(MainApplicationFrame frame) {
        JMenuBar menuBar = new JMenuBar();
        menuBar.add(createLookAndFeelMenu(frame));
        menuBar.add(createTestMenu(frame));
        menuBar.add(createFileMenu(frame));
        return menuBar;
    }

    private static JMenu createLookAndFeelMenu(MainApplicationFrame frame) {
        JMenu lookAndFeelMenu = new JMenu("Режим отображения");
        lookAndFeelMenu.setMnemonic(KeyEvent.VK_V);
        lookAndFeelMenu.getAccessibleContext().setAccessibleDescription(
                "Управление режимом отображения приложения");

        addSystemLookAndFeelItem(lookAndFeelMenu, frame);
        addCrossPlatformLookAndFeelItem(lookAndFeelMenu, frame);

        return lookAndFeelMenu;
    }

    private static JMenu createTestMenu(MainApplicationFrame frame) {
        JMenu testMenu = new JMenu("Тесты");
        testMenu.setMnemonic(KeyEvent.VK_T);
        testMenu.getAccessibleContext().setAccessibleDescription("Тестовые команды");

        addLogMessageItem(testMenu, frame);

        return testMenu;
    }

    private static JMenu createFileMenu(MainApplicationFrame frame) {
        JMenu fileMenu = new JMenu("Файл");
        fileMenu.setMnemonic(KeyEvent.VK_F);
        fileMenu.getAccessibleContext().setAccessibleDescription("Файловые операции");

        addLogMessageItem(fileMenu, frame);

        return fileMenu;
    }

    private static void addLogMessageItem(JMenu menu, MainApplicationFrame frame) {
        JMenuItem addLogMessageItem = new JMenuItem("Сообщение в лог", KeyEvent.VK_S);
        addLogMessageItem.addActionListener(event -> Logger.debug("Новая строка"));
        menu.add(addLogMessageItem);
    }

    private static void addCrossPlatformLookAndFeelItem(JMenu menu, MainApplicationFrame frame) {
        JMenuItem crossplatformLookAndFeel = new JMenuItem("Универсальная схема", KeyEvent.VK_U);
        crossplatformLookAndFeel.addActionListener(event -> {
            frame.setLookAndFeel(UIManager.getCrossPlatformLookAndFeelClassName());
            frame.invalidate();
        });
        menu.add(crossplatformLookAndFeel);
    }

    private static void addSystemLookAndFeelItem(JMenu menu, MainApplicationFrame frame) {
        JMenuItem systemLookAndFeel = new JMenuItem("Системная схема", KeyEvent.VK_S);
        systemLookAndFeel.addActionListener(event -> {
            frame.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
            frame.invalidate();
        });
        menu.add(systemLookAndFeel);
    }
}