import pyzed.sl as sl
import rospy
import rosbag
from sensor_msgs.msg import (
    Image, CompressedImage, CameraInfo
)
from std_msgs.msg import Header

import numpy as np
import cv2


class SVO2ROSBagConverter:
    """
    This class converts a Stereolabs ZED SVO file to a ROS bag file.
    Specifically, it reads the RGB and depth data from the SVO file
    and writes them to a ROS bag file as CompressedImage and Image.
    Additionally, it reads the camera information from the SVO file
    and writes it to the ROS bag as CameraInfo messages.
    """
    def __init__(self, 
                 svo_file_path, 
                 rosbag_file_path,
                 rgb_topic,
                 depth_topic,
                 camera_info_topic,
                 publish_rgb=False,
                 publish_depth=False,
                 publish_camera_info=False):
        self.svo_file_path = svo_file_path
        self.rosbag_file_path = rosbag_file_path
        self.rgb_topic = rgb_topic
        self.depth_topic = depth_topic
        self.camera_info_topic = camera_info_topic
        self.publish_rgb = publish_rgb
        self.publish_depth = publish_depth
        self.publish_camera_info = publish_camera_info

        # Initialize the ZED camera.
        self.zed = sl.Camera()
        self.init_params = sl.InitParameters()
        self.init_params.set_from_svo_file(svo_file_path)
        self.runtime_params = sl.RuntimeParameters()

        # Open the SVO file.
        self.open_camera()

        # Open ROS bag for writing.
        self.bag = rosbag.Bag(
            f=self.rosbag_file_path, 
            mode="w"
        )

    def open_camera(self):
        """
        This method opens the ZED camera with the SVO file.
        """
        status = self.zed.open(self.init_params)
        if status != sl.ERROR_CODE.SUCCESS:
            print(f"Failed to open SVO file: {status}")
            exit(1)
        print("SVO file opened successfully.")

    def read_camera_information(self):
        """
        This method reads camera parameter information 
        from a SVO file using the ZED SDK.
        """
        # Get the camera information (parameters).
        camera_info = self.zed.get_camera_information()
        camera_config = camera_info.camera_configuration
        calibration_params = camera_config.calibration_parameters

        # Extract left camera parameters.
        left_cam = calibration_params.left_cam

        # Intrinsic parameters (K) from the left camera
        fx = left_cam.fx
        fy = left_cam.fy
        cx = left_cam.cx
        cy = left_cam.cy
        K = [
             fx, 0.0, cx,  # fx,  0, cx
            0.0,  fy, cy,  #  0, fy, cy
            0.0, 0.0, 1.0  #  0,  0,  1
        ]

        # Distortion coefficients (D) from the left camera.
        D = left_cam.disto.tolist()[:5]

        # Rectification matrix (R) for 
        # the left camera (identity for no rectification).
        R = [
            1.0, 0.0, 0.0,
            0.0, 1.0, 0.0,
            0.0, 0.0, 1.0
        ]

        # Projection matrix (P) for the left camera.
        P = [
            fx,  0.0,  cx, 0.0,  # fx,  0, cx, tx * fx
            0.0,  fy,  cy, 0.0,  #  0, fy, cy, 0
            0.0, 0.0, 1.0, 0.0   #  0,  0,  1, 0
        ]

        # Extract resolution (width, height) of left camera.
        resolution = (
            left_cam.image_size.width,
            left_cam.image_size.height
        )

        return (
            K, D, R, P, resolution
        )

    def encode_rgb_data(self, 
                        rgb_data, 
                        timestamp,
                        frame_id="camera_rgb_frame"):
        """
        This method encodes a numpy array of RGB data into a 
        sensor_msgs/CompressedImage message.
        """
        # Create a CompressedImage message and publish it.
        msg = CompressedImage()
        msg.header = Header(
            frame_id=frame_id,
            stamp=timestamp,
        )
        _, compressed_frame = cv2.imencode(".jpg", rgb_data)
        msg.format = "jpeg"
        msg.data = np.asarray(compressed_frame).tobytes()
        return msg
    
    def encode_depth_data(self,
                          depth_data,
                          timestamp, 
                          frame_id="camera_depth_frame"):
        """
        This method encodes a numpy array of depth data into a 
        sensor_msgs/Image message.
        """
        # Ensure the depth array is in the correct format
        if not isinstance(depth_data, np.ndarray):
            rospy.logerr("Depth data must be a numpy array.")
            return None
        
        if depth_data.ndim != 2:
            rospy.logerr("Depth data must be a 2D numpy array.")
            return None

        # Create an Image message.
        msg = Image()

        # Set the header.
        msg.header = Header(
            frame_id=frame_id,
            stamp=timestamp,
        )

        # Set image properties.
        msg.height = depth_data.shape[0]
        msg.width = depth_data.shape[1]
        msg.encoding = "32FC1"  # 32-bit floating-point single-channel image
        msg.is_bigendian = False
        msg.step = msg.width * 4  # 4 bytes per pixel for 32FC1

        # Convert the numpy array to bytes 
        # and set the data field.
        msg.data = depth_data.tobytes()

        return msg
    
    def encode_camera_info_data(self, 
                                K, D, R, P, 
                                resolution,
                                timestamp,
                                frame_id="camera_info_frame"):
        """
        This method encodes the camera information extracted
        from the SVO file into CameraInfo message populated with 
        the camera parameters.
        """
        # Create a CameraInfo message.
        msg = CameraInfo()

        # Set the header with the current time and frame ID.
        msg.header = Header(
            frame_id=frame_id,
            stamp=timestamp
        )

        # Set image dimensions.
        msg.width = resolution[0]
        msg.height = resolution[1]

        # Set intrinsic, distortion, rectification, and 
        # projection matrices.
        msg.K = K
        msg.D = D
        msg.R = R
        msg.P = P

        return msg

    def convert(self):
        image = sl.Mat()
        depth = sl.Mat()
        number_of_frames = self.zed.get_svo_number_of_frames()

        for i in range(number_of_frames):
            if self.zed.grab(self.runtime_params) == sl.ERROR_CODE.SUCCESS:
                # Retrieve the left image.
                self.zed.retrieve_image(image, sl.VIEW.LEFT)
                # Retrieve the depth map.
                self.zed.retrieve_measure(depth, sl.MEASURE.DEPTH)

                # Set the timestamp for the messages.
                timestamp_ns = self.zed.get_timestamp(
                    sl.TIME_REFERENCE.IMAGE
                ).get_nanoseconds()
                secs = timestamp_ns // 1_000_000_000
                nsecs = timestamp_ns % 1_000_000_000
                timestamp = rospy.Time(secs, nsecs)

                if self.publish_rgb:
                    # Publish RGB data as ROS message.
                    # Encode the RGB data as ROS message.
                    rgb_data= image.get_data()
                    rgb_data_msg = self.encode_rgb_data(
                        rgb_data=rgb_data,
                        timestamp=timestamp
                    )
                    # Write the RGB data to the ROS bag.
                    self.bag.write(
                        topic=self.rgb_topic, 
                        msg=rgb_data_msg, 
                        t=timestamp
                    )

                if self.publish_depth:
                    # Publish depth data as ROS message.
                    # Encode the depth data as ROS message.
                    depth_data = depth.get_data()
                    depth_data_msg = self.encode_depth_data(
                        depth_data=depth_data,
                        timestamp=timestamp
                    )
                    # Write the depth data to the ROS bag.
                    self.bag.write(
                        topic=self.depth_topic, 
                        msg=depth_data_msg, 
                        t=timestamp
                    )
                    
                if self.publish_camera_info:
                    # Publish camera information as ROS message.
                    # Encode the camera information as ROS message.
                    K, D, R, P, resolution = self.read_camera_information()
                    camera_info_msg = self.encode_camera_info_data(
                        K=K, D=D, R=R, P=P,
                        resolution=resolution,
                        timestamp=timestamp
                    )
                    # Write the camera information to the ROS bag.
                    self.bag.write(
                        topic=self.camera_info_topic, 
                        msg=camera_info_msg, 
                        t=timestamp
                    )
                print(f"Frame {i+1}/{number_of_frames} written to ROS bag.")
            else:
                print(f"Failed to grab frame {i+1}")
                break

    def close(self):
        # Close the camera and ROS bag
        self.zed.close()
        self.bag.close()
        print("SVO file processing completed. ROS bag created.")

if __name__ == "__main__":
    # SVO file path
    svo_file_path = ""  # Replace with your desired SVO file path.

    # ROS bag file path
    rosbag_file_path = ""  # Replace with your desired ROS bag file path.

    # ROS topics
    rgb_topic = ""  # Replace with your desired RGB topic name.
    depth_topic = ""  # Replace with your desired depth topic name.
    camera_info_topic = ""  # Replace with your desired camera info topic name.

    # Publish flags
    publish_rgb = True
    publish_depth = True
    publish_camera_info = True

    # Initialize the converter.
    converter = SVO2ROSBagConverter(
        svo_file_path=svo_file_path, 
        rosbag_file_path=rosbag_file_path,
        rgb_topic=rgb_topic,
        depth_topic=depth_topic,
        camera_info_topic=camera_info_topic,
        publish_rgb=publish_rgb,
        publish_depth=publish_depth,
        publish_camera_info=publish_camera_info
    )

    # Convert the SVO file to ROS bag.
    converter.convert()

    # Close the converter.
    converter.close()
