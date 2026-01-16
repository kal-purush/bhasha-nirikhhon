import java.io.Serializable;


public class CreateSnapshotRequest extends JCSRequest implements Serializable, Cloneable{

    /**
     * <p>
     * The size of the volume, in GiBs.
     * </p>
     * <p>
     * Constraints: 1-16384 for <code>gp2</code>, 4-16384 for <code>io1</code>,
     * 500-16384 for <code>st1</code>, 500-16384 for <code>sc1</code>, and
     * 1-1024 for <code>standard</code>. If you specify a snapshot, the volume
     * size must be equal to or larger than the snapshot size.
     * </p>
     * <p>
     * Default: If you're creating the volume from a snapshot and don't specify
     * a volume size, the default is the snapshot size.
     * </p>
     */
    
    /**
     * <p>
     * The snapshot from which to create the volume.
     * </p>
     */
    private String volumeId;
    
    public CreateSnapshotRequest() {
    }

    public CreateSnapshotRequest(String volumeId) {
        setVolumeId(volumeId);
    }
  
    public void setVolumeId(String VolumeId) {
        this.volumeId = VolumeId;
    }

    

    public String getVolumeId() {
        return this.volumeId;
    }

    
    public CreateSnapshotRequest withVolumeId(String volumeId) {
        setVolumeId(volumeId);
        return this;
    }

    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        if (getVolumeId() != null)
            sb.append("SnapshotId: " + getVolumeId() + ",");
        sb.append("}");
        return sb.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;

        if (obj instanceof CreateSnapshotRequest == false)
            return false;
        CreateSnapshotRequest other = (CreateSnapshotRequest) obj;
        if (other.getVolumeId() == null ^ this.getVolumeId() == null)
            return false;
        if (other.getVolumeId() != null
                && other.getVolumeId().equals(this.getVolumeId()) == false)
            return false;
        return true;
    }

}   