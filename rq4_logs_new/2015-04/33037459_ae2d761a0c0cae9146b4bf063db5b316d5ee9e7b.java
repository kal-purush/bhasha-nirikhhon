package local.deva.kmldroid.model.feature;


import android.net.Uri;

import local.deva.kmldroid.model.*;
import local.deva.kmldroid.model.Object;
import local.deva.kmldroid.model.abstractview.AbstractView;
import local.deva.kmldroid.model.atom.Author;
import local.deva.kmldroid.model.atom.Link;
import local.deva.kmldroid.model.extendeddata.ExtendedData;
import local.deva.kmldroid.model.styleselector.StyleSelector;
import local.deva.kmldroid.model.timeprimtive.TimePrimitive;

public abstract class Feature extends Object {
    private String name;
    private boolean visibility;
    private boolean open;
    private Author author;
    private Link link;
    private String address;
    private String addressDetails;
    private String phoneNumber;
    private String snippet;
    private String description;
    private AbstractView abstractView;
    private TimePrimitive timePrimitive;
    private Uri styleUrl;
    private StyleSelector styleSelector;
    private Region region;
    private ExtendedData extendedData;


}