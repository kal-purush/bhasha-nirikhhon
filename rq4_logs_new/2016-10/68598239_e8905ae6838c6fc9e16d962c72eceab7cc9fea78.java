package com.haulmont.testtask.views.Group.presenters;

import com.haulmont.testtask.models.Group.Group;
import com.haulmont.testtask.views.Main.IViewListener;
import com.vaadin.data.util.BeanItem;

public interface IGroupViewListener extends IViewListener {
    void createData(BeanItem<Group> item);
    void updateData(BeanItem<Group> item);
    void deleteData(BeanItem<Group> item);
}