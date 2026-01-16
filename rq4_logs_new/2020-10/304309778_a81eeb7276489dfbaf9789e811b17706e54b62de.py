
import rclpy
from rclpy.node import Node
from geometry_msgs.msg import Twist

class MoveTurtleTest(Node):

    def __init__(self):
        super().__init__('move_turtle_test')
        self.publisher_test_move = self.create_publisher(Twist, '/turtle1/cmd_vel', 10)   
        timer_period = 1
        self.timer = self.create_timer(timer_period, self.move_test)
        self.i = 0

    def move_test(self):
        #Receiveing the user's input
        print("Let's move your robot")
        speed = float(input("Input your speed:"))
        distance = float(input("Type your distance:"))
        isForward = int(input("Foward?: 1 or 0"))#True or False   

        vel_msg = Twist()
        #Checking if the movement is forward or backwards
        if(isForward):
            vel_msg.linear.x = (speed)
        else:
            vel_msg.linear.x = -1*(speed)
        #Since we are moving just in x-axis
        vel_msg.linear.y = 0.0
        vel_msg.linear.z = 0.0
        vel_msg.angular.x = 0.0
        vel_msg.angular.y = 0.0
        vel_msg.angular.z = 0.0
                       
        t0 = rclpy.clock.Clock().now().seconds_nanoseconds()
        current_distance = 0.0
        while(current_distance < distance):
            #Publish the velocity
            self.publisher_test_move.publish(vel_msg)
            #Takes actual time to velocity calculus
            t1=rclpy.clock.Clock().now().seconds_nanoseconds()
            #Calculates distancePoseStamped
            current_distance= (speed)*(t1[0]-t0[0])
        #After the loop, stops the robot
        vel_msg.linear.x = 0.0
        
        self.publisher_test_move.publish(vel_msg)
        # self.get_logger().info('Publishing: "%d"' % vel_msg.num)  # CHANGE
        self.i += 1

def main(args=None):
    rclpy.init(args=args)

    move_turtle_test = MoveTurtleTest()

    rclpy.spin(move_turtle_test)

    move_turtle_test.destroy_node()
    rclpy.shutdown()

if __name__ == '__main__':
    main()