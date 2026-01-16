import os;
import ConfigParser;

def getOption(file, section, option, default):
    opt = getSection(file, section)[option];
    if opt == None:
        return default;
    else:
        return opt;

def getSection(file, section):
    if os.path.isfile(file) != True:
        file = open(file, "w+");
    Config = ConfigParser.ConfigParser();
    Config.read(file);
    sect = {}
    options = Config.options(section);
    for option in options:
        try:
            sect[option] = Config.get(section, option);
        except:
            sect[option] = None;
    return sect;

def getFile(file):
    if os.path.isfile(file) != True:
        file = open(file, "w+");
        file.close();
    Config = ConfigParser.ConfigParser();
    Config.read(file);
    sects = {};
    opts = {};
    sections = Config.sections();
    for section in sections:
        options = Config.options(section);
        for option in options:
            opts[option] = Config.get(section, option);
        sects[section] = opts
        opts = {}
    return sects;