
#!/usr/bin/python

'''
SETUP:

    -   -->     GND     -->     PIN6
    +   -->     5V      -->     PIN4
    S   -->     GPIO18  -->     PIN12

'''

#import RPi.GPIO as GPIO
import subprocess
import time
import sys
import os
import boto
import boto3
import boto.s3
import boto.sqs

from boto.s3.key import Key
from boto.sqs.message import Message
import threading

sensor = 12

#GPIO.setwarnings(False)
#GPIO.setmode(GPIO.BOARD)
#GPIO.setup(sensor, GPIO.IN)

on = 0
off = 0
flag = 0
pi_flag = 0
def process(s3, s3BucketName, s3OutputPrefix, fileName, workDir):
    s3Bucket = s3.get_bucket(s3BucketName)
    localInputPath = os.path.join(workDir, fileName)
    localOutputPath = os.path.join(workDir, fileName[:-4]+'.txt')
    remoteInputPath = s3InputPrefix + '/' + fileName
    remoteOutputPath = s3OutputPrefix + '/' + fileName[:-4]+'.txt'
    print("Downloading %s from s3://%s/%s ..." % (localInputPath, s3BucketName, remoteInputPath))
    print(remoteInputPath)
    key = s3Bucket.get_key(remoteInputPath)

    # s3 = boto3.client('s3')
    # s3.download_file(s3BucketName, remoteInputPath, localInputPath)
    # key.get_contents_to_filename(workDir+"/"+fileName)
    #subprocess.call(['./darknet','detector','demo','cfg/coco.data','cfg/yolov3-tiny.cfg','yolov3-tiny.weights', localInputPath,'outpt.txt'])
    os.system('./darknet detector demo cfg/coco.data cfg/yolov3-tiny.cfg yolov3-tiny.weights '+localInputPath+' > '+localOutputPath)
    print("Uploading %s to s3://%s/%s ..." % (localOutputPath, s3BucketName, remoteOutputPath))
    key = Key(s3Bucket)
    key.key = remoteOutputPath
    key.set_contents_from_filename(localOutputPath)
    return True

while True:
    #Set input as 1
	i = 1
	#i=GPIO.input(sensor)
	print(i)
	if i == 0:
	    if flag == 1:
	        off = time.time()
	        diff = off - on
	        print 'time: ' + str(diff%60) + ' sec'
	        print ''
	        flag = 0
	    print "No intruders"
	    time.sleep(1)
	elif i == 1:
	    if flag == 0:
	        print "Intruder detected"
	        on = time.time()
	        flag = 1
	        #subprocess.call(['sudo','python','take_snapshot.py','frame.jpg'])
		path = "videos"
		#print("a")
		if not os.path.isdir(path):
			subprocess.call(['sudo','mkdir','videos'])
		#print("b")
		t = str(time.time())
        # Next command is for recording video for Intruder Detected
		#subprocess.call(['raspivid','-o','videos/'+t+'.h264','-t','10000'])
		#time.sleep(11)
        s3 = boto.s3.connect_to_region(awsRegion)
        s3Bucket = s3.get_bucket(s3BucketName)
        if pi_flag == 0:
            thread = threading.Thread(target = processVideo, args=(s3, s3Bucket, t+'.mp4','videos'))
            pi_flag = 1
        else:
            start = time.time()
            #Create a video.h264 file in videos directory
    		subprocess.call(['./send_video.py','videos/video.h264', 'k-ccproj','input','output','TestQueue','us-east-1'])
    		end = time.time()
		    print(str((end-start)))
        if not thread.isAlive():
            pi_flag = 0

		#subprocess.call(['sudo', './facedetect', 'frame.jpg'])
	    time.sleep(0.1)
