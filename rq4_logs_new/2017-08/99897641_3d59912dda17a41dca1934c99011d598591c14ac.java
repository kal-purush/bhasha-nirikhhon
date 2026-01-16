package com.exscan.online.exscan.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by sreed on 8/11/2017.
 */
public class FileExtModelOrderList extends OrderdList {
    public FileExtModelOrderList(int i) {
        super(i);
    }

    @Override
    public Node createNewNode(IModel newmodel) {
        return createNode(newmodel);
    }

    private Node createNode(IModel newmodel) {
        Node newNode = new Node((FileExtModel)newmodel);
        if (numberOfItems < max_size) {
            numberOfItems++;
        }
        return newNode;
    }

    public List<FileExtModel> getFileExtList() {
        ArrayList<FileExtModel> arrayList = new ArrayList<>();
        List<IModel> list = getList();
        for (IModel model: list) {
            arrayList.add((FileExtModel)model);
        }
        return arrayList;
    }
}