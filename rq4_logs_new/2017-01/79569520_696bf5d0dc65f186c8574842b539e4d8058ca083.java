package apriltags;

public interface AprilTagDetection extends org.ros.internal.message.Message {
  static final java.lang.String _TYPE = "apriltags/AprilTagDetection";
  static final java.lang.String _DEFINITION = "int32 id\nfloat64 size\ngeometry_msgs/PoseStamped pose";
  int getId();
  void setId(int value);
  double getSize();
  void setSize(double value);
  geometry_msgs.PoseStamped getPose();
  void setPose(geometry_msgs.PoseStamped value);
}