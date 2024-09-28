import cv2
import rospy
from sensor_msgs.msg import CompressedImage
import numpy as np

class VideoPublisher:
    """
    This class reads a video file and publishes the frames
    to the specified topics at a specified rate.
    """
    def __init__(self, 
                 video_path, 
                 topic_name,
                 frame_size, 
                 rate_hz=10):
        self.video_path = video_path
        self.topic_name = topic_name
        self.frame_size = frame_size
        self.rate_hz = rate_hz
        self.cap = None
        rospy.init_node(
            name="video_publisher", 
            anonymous=True
        )
        self.get_publishers()

    def get_publishers(self):
        if self.topic_name is not None:
            self.image_pub = rospy.Publisher(
                name=self.topic_name, 
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
            frame = cv2.resize(
                frame, 
                self.frame_size
            )
            msg = self.encode_image(frame=frame)

            # Publish the message.
            self.image_pub.publish(msg)
        
        except Exception as e:
            rospy.logerr(f"Error: {str(e)}")

        return

    def run(self):
        # Adjust loop rate.
        rate = rospy.Rate(self.rate_hz)

        if self.open_video():
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

    # Topic name
    topic_name = ""  # Replace with your desired topic name.

    # Loop rate
    rate_hz = 10  # Replace with your desired loop rate.

    # Initialize the VideoPublisher class.
    video_publisher = VideoPublisher(
        video_path=video_path, 
        topic_name=topic_name, 
        rate_hz=rate_hz
    )

    # Run the video publisher node.
    video_publisher.run()

