#!/usr/bin/python3
import json
import os
import subprocess
import signal

from sys import argv, exit

import boto
import boto3
import boto.s3
import boto.sqs

from boto.s3.key import Key
from boto.sqs.message import Message

coco_names = ['person', 'bicycle', 'car', 'motorbike', 'aeroplane', 'bus', 'train', 'truck', 'boat', 'traffic light', 'fire hydrant', 'stop sign', 'parking meter', 'bench', 'bird', 'cat', 'dog', 'horse', 'sheep', 'cow', 'elephant', 'bear', 'zebra', 'giraffe', 'backpack', 'umbrella', 'handbag', 'tie', 'suitcase', 'frisbee', 'skis', 'snowboard', 'sports ball', 'kite', 'baseball bat', 'baseball glove', 'skateboard', 'surfboard', 'tennis racket', 'bottle', 'wine glass', 'cup', 'fork', 'knife', 'spoon', 'bowl', 'banana', 'apple', 'sandwich', 'orange', 'broccoli', 'carrot', 'hot dog', 'pizza', 'donut', 'cake', 'chair', 'sofa', 'pottedplant', 'bed', 'diningtable', 'toilet', 'tvmonitor', 'laptop', 'mouse', 'remote', 'keyboard', 'cell phone', 'microwave', 'oven', 'toaster', 'sink', 'refrigerator', 'book', 'clock', 'vase', 'scissors', 'teddy bear', 'hair drier', 'toothbrush']

def getJobs(workDir, sqsQueueName, awsRegion):
    s3 = boto.s3.connect_to_region(awsRegion)
    sqs = boto.sqs.connect_to_region(awsRegion)
    sqsQueue =  sqs.lookup(sqsQueueName)
    print("Getting messages from SQS queue...")
    messages = sqsQueue.get_messages(wait_time_seconds=20)
    if messages:
        for m in messages:
            job = json.loads(m.get_body())
            print("Message received")
            action = job[0]
            if action == 'process':
                s3BucketName = job[1]
                s3InputPrefix = job[2]
                s3OutputPrefix = job[3]
                fileName = job[4]
                status = process(s3, s3BucketName, s3InputPrefix, s3OutputPrefix, fileName, workDir)
                if (status):
                    print("Message processed correctly ...")
                    m.delete()
    else:
        print("No Messages")

def process(s3, s3BucketName, s3InputPrefix, s3OutputPrefix, fileName, workDir):
    s3Bucket = s3.get_bucket(s3BucketName)
    localInputPath = os.path.join(workDir, fileName)
    localOutputPath = os.path.join(workDir, fileName[:-5]+'.txt')
    remoteInputPath = s3InputPrefix + '/' + fileName
    remoteOutputPath = s3OutputPrefix + '/' + fileName[:-5]+'.txt'
    if not os.path.isdir(workDir):
        os.system('sudo mkdir work && sudo chmod 777 work')
    print("Downloading %s from s3://%s/%s ..." % (localInputPath, s3BucketName, remoteInputPath))
    #print(remoteInputPath)
    key = s3Bucket.get_key(remoteInputPath)

    s3 = boto3.client('s3')
    s3.download_file(s3BucketName, remoteInputPath, localInputPath)
    key.get_contents_to_filename(workDir+"/"+fileName)
    #subprocess.call(['./darknet','detector','demo','cfg/coco.data','cfg/yolov3-tiny.cfg','yolov3-tiny.weights', localInputPath,'outpt.txt'])
    os.system('Xvfb :1 & export DISPLAY=:1 && ./darknet detector demo cfg/coco.data cfg/yolov3-tiny.cfg yolov3-tiny.weights '+localInputPath+' > '+localOutputPath)
    parseOutput(localOutputPath)
    print("Uploading %s to s3://%s/%s ..." % (localOutputPath, s3BucketName, remoteOutputPath))
    key = Key(s3Bucket)
    key.key = remoteOutputPath
    key.set_contents_from_filename(localOutputPath)
    return True

def parseOutput(fileName):
    result = []

    with open(fileName, 'r') as f:
        lines = f.readlines()
        for line in lines:
            result.extend([obj for obj in coco_names if(obj in line and obj not in result)])    
    
    with open(fileName, 'w') as f:
        if(len(result)==0):
            f.write("No object detected")
        else:
            for item in result:
                f.write(item + '\n')


def signal_handler(signal, frame):
    print("Exiting...")
    exit(0)

def main():
    if len(argv) < 4:
        print("Usage: %s <working directory> <SQS queue> <AWS region>" % argv[0])
        exit(1)
    workDir = argv[1]
    #outputExtension = argv[2]
    sqsQueueName = argv[2]
    awsRegion = argv[3]
    #command = argv[5]
    getJobs(workDir, sqsQueueName, awsRegion)

if __name__ == '__main__':

    signal.signal(signal.SIGINT, signal_handler)
    main()
