import cv2
import rospy
from sensor_msgs.msg import CompressedImage
import numpy as np


class VideoPublisher:
    """
    This class reads a video file and publishes the frames
    to the specified topics at a specified rate.
    It is a multi-camera version of the 'video2ros.py' 
    script's VideoPublisher class.
    """
    def __init__(self, 
                 video_path=None, 
                 cam1_topic_name=None,
                 cam2_topic_name=None,
                 cam3_topic_name=None, 
                 cam1_frame_size=(1920, 1080),
                 cam2_frame_size=(1920, 1080),
                 cam3_frame_size=(1920, 1080),
                 rate_hz=10):
        self.video_path = video_path
        self.cam1_topic_name = cam1_topic_name
        self.cam2_topic_name = cam2_topic_name
        self.cam3_topic_name = cam3_topic_name
        self.cam1_frame_size = cam1_frame_size
        self.cam2_frame_size = cam2_frame_size
        self.cam3_frame_size = cam3_frame_size
        self.rate_hz = rate_hz
        self.cap = None
        rospy.init_node(
            name="video_publisher", 
            anonymous=True
        )
        self.get_publishers()
    
    def get_publishers(self):
        # Camera 1 RGB publisher
        if self.cam1_topic_name is not None:
            self.cam1_image_pub = rospy.Publisher(
                name=self.cam1_topic_name, 
                data_class=CompressedImage, 
                queue_size=30
            )
        # Camera 2 RGB publisher
        if self.cam2_topic_name is not None:
            self.cam2_image_pub = rospy.Publisher(
                name=self.cam2_topic_name, 
                data_class=CompressedImage, 
                queue_size=30
            )
        # Camera 3 RGB publisher
        if self.cam3_topic_name is not None:
            self.cam3_image_pub = rospy.Publisher(
                name=self.cam3_topic_name, 
                data_class=CompressedImage, 
                queue_size=30
            )

        return

    def open_video(self):
        self.cap = cv2.VideoCapture(self.video_path)
        if not self.cap.isOpened():
            rospy.logerr("Error: Cannot open video file.")
            return False
        return True
    
    def encode_image(self, frame):
        """
        This method converts an array of tracks to a 'CompressedImage' 
        message to publish as a ROS message.
        """
        # Convert the RGB image to JPEG compressed format.
        _, compressed_data = cv2.imencode(".jpg", frame)

        # Create a CompressedImage message and publish it.
        msg = CompressedImage()
        msg.format = "jpeg"
        msg.data = np.array(compressed_data).tobytes()

        return msg
    
    def publish_frames(self):
        try:
            # Read the next frame from the video stream.
            ret, frame = self.cap.read()
            if not ret:
                raise Exception("Unable to read video stream.")

            # Convert the frame to a ROS CompressedImage message.
            cam1_frame = cv2.resize(
                frame, 
                self.cam1_frame_size
            )
            cam2_frame = cv2.resize(
                frame, 
                self.cam2_frame_size
            )
            cam3_frame = cv2.resize(
                frame, 
                self.cam3_frame_size
            )
            cam1_msg = self.encode_image(frame=cam1_frame)
            cam2_msg = self.encode_image(frame=cam2_frame)
            cam3_msg = self.encode_image(frame=cam3_frame)

            # Publish messages.
            self.cam1_image_pub.publish(cam1_msg)
            self.cam2_image_pub.publish(cam2_msg)
            self.cam3_image_pub.publish(cam3_msg)

        except Exception as e:
            rospy.logerr(f"Error: {e}")

        return

    def run(self):
        # Adjust loop rate.
        rate = rospy.Rate(self.rate_hz)

        if not self.open_video():
            while not rospy.is_shutdown():
                # Publish frames.
                self.publish_frames()

                # Sleep to maintain the desired publishing rate.
                rate.sleep()

            # Release the video capture object.
            self.cap.release()
        
        return
    

if __name__ == '__main__':
    # Video path
    video_path = ""  # Replace with your desired video path.

    # Camera RGB topic names
    cam1_topic_name = ""  # Replace with your desired topic name.
    cam2_topic_name = ""  # Replace with your desired topic name.
    cam3_topic_name = ""  # Replace with your desired topic name.

    # Camera frame sizes
    cam1_frame_size = (1920, 1080)  # Replace with your camera 1's frame size.
    cam2_frame_size = (1920, 1080)  # Replace with your camera 2's frame size.
    cam3_frame_size = (1920, 1080)  # Replace with your camera 3's frame size.

    # Loop rate
    rate_hz = 10  # Replace with your desired loop rate.

    # Initialize the VideoPublisher class.
    video_publisher = VideoPublisher(
        video_path=video_path, 
        cam1_topic_name=cam1_topic_name,
        cam2_topic_name=cam2_topic_name,
        cam3_topic_name=cam3_topic_name, 
        cam1_frame_size=cam1_frame_size,
        cam2_frame_size=cam2_frame_size,
        cam3_frame_size=cam3_frame_size,
        rate_hz=rate_hz
    )

    # Run the video publisher node.
    video_publisher.run()

