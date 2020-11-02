import botocore    # python 3.7
import boto3
import datetime
import dateutil

ec = boto3.client('ec2', 'ap-northeast-2')
retention_days = 14

def create_snapshot(retention_days):
    reservations = ec.describe_instances(
        Filters=[
            { 'Name': 'tag:Backup', 'Values': ['Y'] },
            { 'Name': 'instance-state-name', 'Values': ['running'] }
        ]
    ).get(
        'Reservations', []
    )

    instances = sum(
        [
            [i for i in r['Instances']]
            for r in reservations
        ], [])

    print("Found %d instances that need backing up" % len(instances))

    for instance in instances:
        instance_name = [res['Value'] for res in instance['Tags'] if res['Key'] == 'Name'][0]
        print("Instance name:" + instance_name)
        
        for volume in instance['BlockDeviceMappings']:
            if volume.get('Ebs', None) is None:
                continue
            vol_id = volume['Ebs']['VolumeId']
            print("Found EBS volume %s on instance %s" % (
                vol_id, instance['InstanceId']))
            
            description = '%s-%s' % (instance_name, datetime.datetime.now().strftime("%Y%m%d"))
            deleteOn = datetime.datetime.now() + datetime.timedelta(days=retention_days)
            
            response=ec.create_snapshot(
                Description=description,
                VolumeId=vol_id,
                TagSpecifications = [{
                    'ResourceType': 'snapshot',
                    'Tags': [
                        {'Key': 'Name', 'Value': description},
                        {'Key': 'DeleteOn', 'Value': str(deleteOn.date())}
                    ]
                }]
            )
            
            snapshot_id = response['SnapshotId']
            snapshot_complete_waiter = ec.get_waiter('snapshot_completed')
            
            try:
                if response:
                    print("Snapshot created with description [%s]" % description)
                    
                # Set Basic settings timeout large
                snapshot_complete_waiter.wait(
                    SnapshotIds=[snapshot_id], 
                    WaiterConfig={
                        'Delay': 10,
                        'MaxAttempts': 2
                    }
                )
            except botocore.exceptions.WaiterError as e:
                print(e)


def delete_snapshot(retention_days):
    # Get the 14 days old date
    timeLimit=datetime.datetime.now() - datetime.timedelta(days=14)  
    ebsAllSnapshots = ec.describe_snapshots(OwnerIds=['self'])
    
    for snapshot in ebsAllSnapshots['Snapshots']:
        strdate = snapshot['StartTime'].date()
        deleteOn = snapshot['StartTime'] + datetime.timedelta(days=retention_days)
        deleteOntag = [tag['Key'] for tag in snapshot.get('Tags', 'N') if tag != 'N' and tag['Key'] == 'DeleteOn']
        
        nametemp = [tag['Value'] for tag in snapshot.get('Tags', 'N') if tag != 'N' and tag['Key'] == 'Name']
        if nametemp: name = nametemp[0]
      
        if(deleteOntag and snapshot['Description'].split(' ')[0] != 'Created'):
            if strdate <= timeLimit.date():
                print("Deleting Snapshot %s, strdate: %s, deleteOn: %s "  %(name, strdate, deleteOn.date()))
                
                ec.delete_snapshot(SnapshotId=snapshot['SnapshotId']) 
            else:
                # this section will have all snapshots which is created before 14 days
                print("Remain Snapshot %s, strdate: %s, deleteOn: %s "  %(name, strdate, deleteOn.date()))


def lambda_handler(event, context):
    create_snapshot(retention_days)
    delete_snapshot(retention_days)
    
    return 'successful'