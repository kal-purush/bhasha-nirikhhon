package frc.robot.subsystems;

import java.util.Optional;

import javax.swing.TransferHandler.TransferSupport;

import org.littletonrobotics.junction.Logger;
import org.photonvision.EstimatedRobotPose;
import org.photonvision.PhotonCamera;
import org.photonvision.PhotonPoseEstimator;
import org.photonvision.PhotonUtils;
import org.photonvision.PhotonPoseEstimator.PoseStrategy;
import org.photonvision.targeting.PhotonPipelineResult;
import org.photonvision.targeting.PhotonTrackedTarget;

import edu.wpi.first.apriltag.AprilTagFieldLayout;
import edu.wpi.first.apriltag.AprilTagFields;
import edu.wpi.first.math.estimator.PoseEstimator;
import edu.wpi.first.math.geometry.Pose2d;
import edu.wpi.first.math.geometry.Pose3d;
import edu.wpi.first.math.geometry.Rotation2d;
import edu.wpi.first.math.geometry.Rotation3d;
import edu.wpi.first.math.geometry.Transform2d;
import edu.wpi.first.math.geometry.Transform3d;
import edu.wpi.first.math.geometry.Translation3d;
import edu.wpi.first.wpilibj2.command.SubsystemBase;
import edu.wpi.first.wpilibj2.command.sysid.SysIdRoutine;
import frc.robot.Constants;
import frc.robot.utilities.MathUtils;
import frc.robot.utilities.LimelightHelpers.LimelightTarget_Fiducial;

public class PhotonVisionSubsystem extends SubsystemBase implements VisionSubsystem {

    private PhotonCamera photonCamera;
    private PhotonPipelineResult result;
    private AprilTagFieldLayout aprilTagFieldLayout;
    private PhotonPoseEstimator photonPoseEstimator;
    private Pose3d robotPose = new Pose3d();

    public PhotonVisionSubsystem() {
        photonCamera = new PhotonCamera("USB_Camera");
        aprilTagFieldLayout = AprilTagFields.k2024Crescendo.loadAprilTagLayoutField();
        photonPoseEstimator = new PhotonPoseEstimator(aprilTagFieldLayout, PoseStrategy.MULTI_TAG_PNP_ON_COPROCESSOR,
                photonCamera, new Transform3d());
    }

    @Override
    public boolean CameraConnected() {
        return true;
    }

    @Override
    public boolean getTargetVisible(int id) {
        for (PhotonTrackedTarget p : result.targets) {
            if (p.getFiducialId() == id)
                return true;
        }
        return false;
    }

    @Override
    public Pose2d getRobotFieldPoseByTag(int id) {
        Transform3d tagToCamera = getFiducial(id).getBestCameraToTarget();
        return aprilTagFieldLayout.getTagPose(id).get().transformBy(tagToCamera.inverse()).toPose2d();
    }

    @Override
    public Pose2d getRobotPose() {
        return transformPhotonVisionToField(robotPose.toPose2d());
    }

    @Override
    public Pose3d getTargetPoseInRobotSpace(int id) {
        Transform3d pose = getFiducial(id).getBestCameraToTarget();
        return new Pose3d(pose.getY(), pose.getZ(), -pose.getX(), pose.getRotation());
    }

    @Override
    public Pose3d getTargetSpacePose(int id) {
        Transform3d pose = getFiducial(id).getBestCameraToTarget().inverse();
        return new Pose3d(pose.getY(), pose.getZ(), -pose.getX(), pose.getRotation());
    }

    @Override
    public void switchPipeline(int pipeline) {
        photonCamera.setPipelineIndex(pipeline);
    }

    private PhotonTrackedTarget getFiducial(int id) {
        for (PhotonTrackedTarget p : result.targets) {
            if (p.getFiducialId() == id)
                return p;
        }
        return null;
    }

    @Override
    public boolean seeingAnything() {
        return result.hasTargets();
    }

    @Override
    public LimelightTarget_Fiducial limelightTarget_Fiducial(int id) {
        throw new UnsupportedOperationException("Unimplemented method 'limelightTarget_Fiducial'");
    }

    @Override
    public double getTargetX(int id) {
        return getFiducial(id).getYaw();
    }

    private Pose2d transformPhotonVisionToField(Pose2d in) {
        return new Pose2d(
                in.getX() - (Constants.FieldConstants.fieldWidth / 2),
                in.getY() - (Constants.FieldConstants.fieldLength / 2),
                in.getRotation());
    }

    @Override
    public void periodic() {
        result = photonCamera.getLatestResult();
        Optional<EstimatedRobotPose> estimatedPose = photonPoseEstimator.update();
        if (estimatedPose.isPresent()) {
            robotPose = estimatedPose.get().estimatedPose;
        }
    }

}