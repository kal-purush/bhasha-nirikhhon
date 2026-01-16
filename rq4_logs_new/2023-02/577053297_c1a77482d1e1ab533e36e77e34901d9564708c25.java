package arcadia.domainobjects.Macros;

public class CustomLabelTags {
    private String tagconnectorSpliceLabel;
    private String tagHarnessLabel;

    public CustomLabelTags(String tagconnectorSpliceLabel, String tagHarnessLabel) {
        this.tagconnectorSpliceLabel = tagconnectorSpliceLabel;
        this.tagHarnessLabel = tagHarnessLabel;
    }

    public CustomLabelTags() {

    }

    public String getTagconnectorSpliceLabel() {
        return tagconnectorSpliceLabel;
    }

    public void setTagconnectorSpliceLabel(String tagconnectorSpliceLabel) {
        this.tagconnectorSpliceLabel = tagconnectorSpliceLabel;
    }

    public String getTagHarnessLabel() {
        return tagHarnessLabel;
    }

    public void setTagHarnessLabel(String tagHarnessLabel) {
        this.tagHarnessLabel = tagHarnessLabel;
    }
}