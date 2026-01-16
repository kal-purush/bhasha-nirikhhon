package com.redhat.devtools.intellij.jkube;

import com.intellij.icons.AllIcons;
import com.intellij.openapi.util.IconLoader;

import javax.swing.Icon;

public interface Icons {
    public static final Icon CLUSTER = IconLoader.getIcon("/icons/cubes-solid.svg", Icons.class);

    public static final Icon SERVICE = IconLoader.getIcon("/icons/layer-group-solid.svg", Icons.class);

    public static final Icon PORT = IconLoader.getIcon("/icons/diagram-project-solid.svg", Icons.class);

    public static final Icon REMOTE = IconLoader.getIcon("/icons/folder-regular.svg", Icons.class);
}