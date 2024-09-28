import rospy
from sensor_msgs.msg import CompressedImage
import cv2
import numpy as np
import os


LOOP_RATE = 30


class ROSVideoWriter:
    def __init__(self,
                 topic=None,
                 video_path=None, 
                 fps=30,
                 frame_size=[1920, 1080],
                 rate_hz=10):
        """
        Video writer class to write video data from a ROS topic.
        """
        self.topic = topic
        self.rgb_data = None

        self.video_path = video_path
        if not os.path.exists(os.path.dirname(self.video_path)):
            os.makedirs(os.path.dirname(self.video_path))
            print(f"Created directory: {os.path.dirname(self.video_path)}")

        self.fourcc = cv2.VideoWriter_fourcc(*"mp4v")
        self.fps = fps

        self.frame_size = frame_size
        self.width = self.frame_size[0]
        self.height = self.frame_size[1]

        self.rate_hz = rate_hz

        # Define video writer.
        self.video_writer = cv2.VideoWriter(
            filename=self.video_path, 
            fourcc=self.fourcc, 
            fps=self.fps, 
            frameSize=self.frame_size
        )

        # Initialize ROS node.
        rospy.init_node(
            name="video_writer", 
            anonymous=True
        )

        # Define subscribers.
        rospy.Subscriber(
            name=self.topic, 
            data_class=CompressedImage, 
            callback=self.rgb_topic_callback
        )

    def rgb_topic_callback(self, 
                           msg,
                           frame_size=(1920, 1080)):
        # Callback function for RGB topic.
        rospy.loginfo(
            f"Received RGB data from RGB topic."
        )
        
        # Preprocess the RGB Attribute to store RGB data.
        rgb_data = np.frombuffer(
            buffer=msg.data,
            dtype=np.uint8
        )
        rgb_data = cv2.imdecode(
            buf=rgb_data,
            flags=cv2.IMREAD_COLOR
        )
        
        # Update the RGB data attribute.
        resized_rgb_data = cv2.resize(
            rgb_data, 
            frame_size
        )
        self.rgb_data = resized_rgb_data

    def cleanup(self):
        if self.video_writer:
            self.video_writer.release()
        cv2.destroyAllWindows()

    def run(self):
        # Adjust loop rate.
        rate = rospy.Rate(self.rate_hz)

        while not rospy.is_shutdown():
            self.generate_video()
            self.reset()
            rate.sleep()
        
        self.cleanup()
        print(f"Saved video to: {self.video_path}")

    def reset(self):
        self.rgb_data = None

    def generate_video(self):
        if self.rgb_data is not None:
            try:
                rospy.loginfo(f"RGB frame size: {self.rgb_data.shape}")
                self.video_writer.write(self.rgb_data)
                rospy.loginfo(f"Writing frame to video.")
            except Exception as e:
                rospy.logerr(f"Error: {e}")
        return
    

if __name__ == "__main__":
    # Define the input and output parameters.
    rgb_topic = ""  # Replace with your input image topic.
    video_path = ""  # Replace with your output video path.
    fps = 10  # Replace with your desired FPS.
    frame_size = [1920, 1080]  # Replace with your desired frame size.
    rate_hz = 10  # Replace with your desired loop rate.

    # Initialize the ROSVideoWriter class.
    ros_video_writer = ROSVideoWriter(
        topic=rgb_topic,
        video_path=video_path,
        fps=fps,
        frame_size=frame_size,
        rate_hz=rate_hz
    )

    # Run the ROSVideoWriter class.
    ros_video_writer.run()