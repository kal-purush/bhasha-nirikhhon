package DataTypes;

public class EnumeratorType extends DataType<String> implements UserFriendly {

    public EnumeratorType(String name, boolean isInput) {
        super(name, name.toLowerCase().replace('_',' '), Type.ENUM, isInput);
    }

    public EnumeratorType(String enumString, String name, boolean isInput){super(name, name.toLowerCase().replace('_',' '), enumString, Type.ENUM, isInput);}

    @Override
    public String getPresentableString() {
        return getData();
    }

    @Override
    public void setData(String dataStr) {super.setData(dataStr);}
}