import rospy
import rosbag
from sensor_msgs.msg import CompressedImage

class RosbagPublisher:
    def __init__(self, bag_file, topic1, topic2, rate_hz):
        self.bag_file = bag_file
        self.topic1 = topic1
        self.topic2 = topic2
        self.rate_hz = rate_hz

        # Initialize the ROS node.
        rospy.init_node(
            name="rosbag_publisher_node", 
            anonymous=True
        )

        # Publishers for the topics.
        self.topic1_pub = rospy.Publisher(
            name=topic1, 
            data_class=CompressedImage, 
            queue_size=10
        )
        self.topic2_pub = rospy.Publisher(
            name=topic2, 
            data_class=CompressedImage, 
            queue_size=10
        )

        # Lists to store messages from the bag file.
        self.topic1_msgs = []
        self.topic2_msgs = []

        # Read messages from the bag file.
        self.read_bag()

    def read_bag(self):
        try:
            with rosbag.Bag(self.bag_file, 'r') as bag:
                for topic, msg, t in bag.read_messages(topics=[self.topic1]):
                    if topic == self.topic1:
                        self.topic1_msgs.append((msg, t))
                for topic, msg, t in bag.read_messages(topics=[self.topic2]):
                    if topic == self.topic2:
                        self.topic2_msgs.append((msg, t))
        except Exception as e:
            rospy.logerr("Failed to read from bag file: %s" % str(e))

        err_msg = "Mismatch in the number of messages between the two topics." + \
            f" {len(self.topic1_msgs)} messages in {self.topic1} and " + \
            f"{len(self.topic2_msgs)} messages in {self.topic2}." + \
            " Please check the bag file."
        assert len(self.topic1_msgs) == len(self.topic2_msgs), err_msg

    def run(self):
        rate = rospy.Rate(self.rate_hz)
        while not rospy.is_shutdown() and self.topic1_msgs and self.topic2_msgs:
            try:
                # Publish the first message from each list.
                topic1_msg, _ = self.topic1_msgs.pop(0)
                print(f"Publishing message from {self.topic1}.")
                topic2_msg, _ = self.topic2_msgs.pop(0)
                print(f"Publishing message from {self.topic2}.")
                self.topic1_pub.publish(topic1_msg)
                self.topic2_pub.publish(topic2_msg)
            except Exception as e:
                rospy.logerr("Failed to publish messages: %s" % str(e))
            
            rate.sleep()

        return
    

if __name__ == '__main__':
    # Define the parameters.
    par_dir = ""
    bag_file = ""  # Replace with your bag file path.
    topic1 = ""  # Replace with your first topic.
    topic2 = ""  # Replace with your second topic.
    rate_hz = 10  # replace with your desired publishing rate.

    # Initialize the ROS node.
    node = RosbagPublisher(
        bag_file=bag_file, 
        topic1=topic1, 
        topic2=topic2, 
        rate_hz=rate_hz
    )
    
    # Publish the messages.
    node.run()
