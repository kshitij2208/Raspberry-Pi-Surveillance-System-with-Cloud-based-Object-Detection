#!usr/bin/python

import boto3
import json
import paramiko
import threading
import time

import boto.s3
import boto.sqs

from boto.s3.key import Key
from boto.sqs.message import Message

import sys
from time import sleep

MASTER_ID = 'i-0152faa7cf5c379b2'

def getLengthOfQ(client, sqsQueueUrl):
    response = client.get_queue_attributes(QueueUrl=sqsQueueUrl,AttributeNames=['ApproximateNumberOfMessages',])
    response = int(response['Attributes']['ApproximateNumberOfMessages'])
    return response

def getNumberOfInstances(ec2):
    runningCount = 0
    stoppedCount = 0
    for instance in ec2.instances.all():
        if(instance.state['Name']=='running' and instance.id != MASTER_ID):
            runningCount+=1
        elif(instance.state['Name']=='stopped' and instance.id != MASTER_ID):
            stoppedCount+=1

    return runningCount, stoppedCount

def getRunningIds(ec2):
    ids = []
    for instance in ec2.instances.all():
        if(instance.state['Name']=='running' and instance.id != MASTER_ID):
            ids.append(instance.id)
    return ids

def getStoppedIds(ec2):
    ids = []
    for instance in ec2.instances.all():
        if(instance.state['Name']=='stopped' and instance.id != MASTER_ID):
            ids.append(instance.id)
    return ids

def processVideo(ec2, instance_id):
    key = paramiko.RSAKey.from_private_key_file('CCProj1.pem')
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    instance = [i for i in ec2.instances.filter(InstanceIds=[instance_id])][0]
    while(True):
        try:
            client.connect(hostname=instance.public_ip_address, username="ubuntu", pkey=key, timeout=30)
	    print("Connecting to instance "+str(instance.id))
            sin ,sout ,serr = client.exec_command('Xvfb :1 & export DISPLAY=:1 && cd /home/ubuntu/darknet && python get_video.py work TestQueue us-east-1',timeout = 180) # Ideally get_video should download video, process it and upload results itself
            exit_status = sout.channel.recv_exit_status()
	    #sin ,sout ,serr = client.exec_command('ls')
	    print(exit_status)
            print(sout.read())
            client.close()
            break
        except Exception as e:
            print("Reattempting to connect "+str(e))
            sleep(10)

def main():

    if len(sys.argv) < 2:
        print("Usage: %s <SQS queue> <AWS region>" % sys.argv[0])
        exit(1)
    sqsQueueUrl = sys.argv[1]
    awsRegion = sys.argv[2]

    ec2 = boto3.resource('ec2')
    client = boto3.client('sqs')
    # sqs = boto.sqs.connect_to_region(awsRegion)
    # sqsQueue =  sqs.lookup(sqsQueueName)

    threads = []
    busyIds = []

    while(True):

        # Get the length of the sqs queue
        qLength = getLengthOfQ(client,sqsQueueUrl)
        nRunning, nStopped = getNumberOfInstances(ec2)
	print(qLength,nRunning,nStopped)
	#(5,3,7)
        # increase instances
        if qLength>nRunning-len(busyIds):

            stoppedIds = getStoppedIds(ec2) # Get a list of stopped instance ids
	    #print(len(stoppedIds))
            # Number of instances to be started is the min of #stopped instances and #Idle Running Instances
            nStart = min(nStopped, qLength-(nRunning-len(busyIds)))
            #print(nStart)
	    ec2.instances.filter(InstanceIds = stoppedIds[:nStart]).start()
	    print("Started "+str(stoppedIds[:nStart])+" instances")
	    time.sleep(30)

        # decrease instances
        elif qLength<nRunning-len(busyIds):
            runningIds = getRunningIds(ec2) # Get a list of stopped instance ids
            idleIds = [ id for id in runningIds if id not in busyIds]

            # Stop all Idle instances
            ec2.instances.filter(InstanceIds = idleIds[:len(idleIds)-qLength]).stop()
	    print("Stopped "+str(idleIds[:len(idleIds)-qLength])+" instances")
            time.sleep(30)

        for runningId in getRunningIds(ec2):
            if runningId not in busyIds:
                t = threading.Thread(name=runningId, target = processVideo, args=(ec2, runningId))
                threads.append(t)
                busyIds.append(runningId)
		t.start()

        updated_threads = []
        for t in threads:
            if not t.isAlive():
                busyIds.remove(t.getName())
            else:
                updated_threads.append(t)

        threads = updated_threads
        sleep(60)

"""
def main():

    if len(sys.argv) < 2:
        print("Usage: %s <SQS queue> <AWS region>" % sys.argv[0])
        exit(1)
    sqsQueueUrl = sys.argv[1]
    awsRegion = sys.argv[2]

    ec2 = boto3.resource('ec2')
    sqs_client = boto3.client('sqs')

    while(True):
        print(processVideo(ec2, 'i-0152faa7cf5c379b2'))
        sleep(10)
"""

if __name__ == '__main__':
    main()
