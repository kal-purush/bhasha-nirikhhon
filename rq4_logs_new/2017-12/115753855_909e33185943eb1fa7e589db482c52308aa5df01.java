package org.firstinspires.ftc.teamcode;
import com.qualcomm.robotcore.eventloop.opmode.OpMode;
import com.qualcomm.robotcore.eventloop.opmode.TeleOp;
import com.qualcomm.robotcore.util.ElapsedTime;


@TeleOp(name="TeleBot", group="TeleOps")
public class TeleBot extends OpMode{
  private ElapsedTime time = new ElapsedTime();
  public static final ExampleSubsystem exampleSubsystem = ExampleSubsystem.getInstance();
  private static OpMode[] subsystems = {exampleSubsystem};
  private static IReportable[] reportables = {exampleSubsystem};
  private void reportLoop(){
    for(IReportable rp : reportables){
      rp.report()
    }
    telemetry.addData("Elapsed Time:",time)
  }
  @Override
  public void init(){
    for(OpMode op : subsystems){
      op.init();
    }
  }
  @Override
  public void init_loop(){
    for(OpMode op : subsystems){
      op.init_loop();
    }
  }
  @Override
  public void start(){
    for(OpMode op : subsystems){
      op.start();
    }
    time.reset();
  }
  @Override
  public void loop(){
    for(OpMode op : subsystems){
      op.loop();
    }
    reportLoop();
  }
  @Override
  public void stop(){
    for(OpMode op : subsystems){
      op.stop();
    }
  }
}