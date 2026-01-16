package com.goit.petStoreProject.controller.post.pet;

import com.goit.petStoreProject.controller.Command;
import com.goit.petStoreProject.controller.CommanderUtils;
import com.goit.petStoreProject.model.Utils;

import java.io.File;

public class PostPetImage implements Command {
    private CommanderUtils utils = new CommanderUtils();
    private String SUFFIX = "/uploadImage/";

    @Override
    public boolean execute() {
        utils.getView().write("input pet id");
        long id = Long.parseLong(utils.getView().read());
        utils.getView().write("input text");
        String text = utils.getView().read();
        utils.getView().write("input path to file,\n" +
                " for example 'src/main/resources/example.jpeg'");
        String path = utils.getView().read();
        File file = new File(path);
        Utils.postFile(String.format("%s%s%d%s", Utils.URL, Utils.PET_SUFFIX, id, SUFFIX), file, text, utils.getView());
        return utils.isContinue();
    }

    @Override
    public String commandName() {
        return "image";
    }

    @Override
    public String commandDescription() {
        return "post new image to pet by id";
    }
}