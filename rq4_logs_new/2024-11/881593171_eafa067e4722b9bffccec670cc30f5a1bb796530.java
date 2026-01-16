package org.thebungine.engine.layer;

import imgui.ImGui;
import imgui.ImGuiIO;
import imgui.flag.ImGuiBackendFlags;
import imgui.flag.ImGuiConfigFlags;
import imgui.gl3.ImGuiImplGl3;
import imgui.glfw.ImGuiImplGlfw;
import org.thebungine.engine.Application;

import static org.lwjgl.glfw.GLFW.glfwGetCurrentContext;
import static org.lwjgl.glfw.GLFW.glfwMakeContextCurrent;

@SuppressWarnings("unused")
public class ImguiLayer extends Layer {

    private final ImGuiImplGlfw imGuiGlfw = new ImGuiImplGlfw();
    private final ImGuiImplGl3 imGuiGl3 = new ImGuiImplGl3();

    private ImGuiIO io;

    @Override
    public void onAttach() {
        if(Application.getInstance().getWindow() == null) {
            throw new IllegalStateException("Window was not set in the current application.");
        }

        ImGui.createContext();

        this.io = ImGui.getIO();
        io.addBackendFlags(ImGuiBackendFlags.HasMouseCursors);
        io.addBackendFlags(ImGuiBackendFlags.HasSetMousePos);

        io.addConfigFlags(ImGuiConfigFlags.DockingEnable);
        io.addConfigFlags(ImGuiConfigFlags.ViewportsEnable);

        io.setDisplaySize(1920, 1080);

        imGuiGlfw.init(Application.getInstance().getWindow().getWindowId(), false);
        imGuiGl3.init("#version 130");

        ImGui.styleColorsDark();
    }

    @Override
    public void onDeAttach() {
        imGuiGl3.dispose();
        imGuiGlfw.dispose();

        ImGui.destroyContext();
    }

    @Override
    public void onUpdate() {
        imGuiGlfw.newFrame();
        ImGui.newFrame();
        ImGui.dockSpaceOverViewport();

        ImGui.showDemoWindow();

        ImGui.render();
        imGuiGl3.renderDrawData(ImGui.getDrawData());

        if (io.hasConfigFlags(ImGuiConfigFlags.ViewportsEnable)) {
            final long backupWindowPtr = glfwGetCurrentContext();
            ImGui.updatePlatformWindows();
            ImGui.renderPlatformWindowsDefault();
            glfwMakeContextCurrent(backupWindowPtr);
        }
    }

    @Override
    public String getName() {
        return "ImGUI Layer";
    }
}