#connect
$ssh -i <private key file> ubuntu@dataproc -vvv

# copy file over ssh
$scp -i <private key file> -r crimes1 ubuntu@dataproc:/home/ubuntu/crime_report


#create dir in hdfs
$hadoop fs -mkdir /user/ubuntu

#upload file to hdfs
$ hadoop fs -put crime.csv /user/ubuntu/

#
$hadoop fs -ls <dir>

#yarn check application list
$yarn application -list
$yarn application kill -<app id>