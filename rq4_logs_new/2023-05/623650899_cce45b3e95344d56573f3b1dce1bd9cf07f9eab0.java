package DataTypes;

public class JsonType extends DataType<String> implements UserFriendly{
    public JsonType(String name, boolean isInput) {
        super(name, name.toLowerCase().replace('_',' '), Type.JSON, isInput);
    }

    public JsonType(String json, String name, boolean isInput){super(name, name.toLowerCase().replace('_',' '), json, Type.JSON, isInput);}

    @Override
    public String getPresentableString() {
        return getData();
    }

    @Override
    public void setData(String dataStr) {
        super.setData(dataStr);
    }
}