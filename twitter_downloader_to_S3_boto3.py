# Takes tweets and a designated csv file and writes them to it and save it to S3
# designed to run on EC2 instance
# Import packages and config
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from twitter_credentials1 import consumer_key, consumer_secret, access_token, access_token_secret,ACCESS_ID, ACCESS_KEY

import csv
import time
import datetime
import boto3
import logging
from botocore.exceptions import ClientError

class StdOutListener(StreamListener):
    def __init__(self):
        super(StdOutListener, self).__init__()
        self.bucket = 'twitter-bucket-jingyusu'
        self.current_date = datetime.datetime.now()
        self.file_name = 'stream_covid-19.csv'
        self.object_name = ''
        self.generate_new_object_name()
        self.start_file()

    def generate_new_object_name(self):
        self.object_name =  'stream Covid-19 {0}-{1}.csv'.format(self.current_date.month, self.current_date.day)
        
    def start_file(self):
        self.csvw = csv.writer(open(self.file_name, "w"))
        self.csvw.writerow(['twitter_id', 'name', 'created_at', 'followers_count','place', 'text'])
        print('started new file')

    def upload_file(self):
        """Upload a file to an S3 bucket
    
        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """
    
        # If S3 object_name was not specified, use file_name
        if self.object_name is None:
            self.object_name = self.file_name
    
        # Upload the file
        # remember to set the creditenals with 'aws configure' command 
        s3_client = boto3.client('s3')
        # s3_client = boto3.client('s3',
        #     aws_access_key_id=ACCESS_ID,
        #     aws_secret_access_key=ACCESS_KEY)

        try:
            response = s3_client.upload_file(self.file_name, self.bucket, self.object_name)
            print('upload_successful')
        except ClientError as e:
            logging.error(e)
            # print(e)
            return False
        return True


    def on_status(self, status):
        # Filtering English language tweets from users with more than 500 followers
        if (status.lang == "en") and (status.user.followers_count >= 500):# and 'RT @' not in status.text:
            # Creating this formatting so when exported to csv the tweet stays on one line
            tweet_text = "'" + status.text.replace('\n', ' ') + "'"
            self.csvw.writerow([status.id, 
                           status.user.screen_name,
                           status.created_at,
                           status.user.followers_count,
                           status.place,
                           tweet_text])
            # create a new file each day
            if datetime.datetime.now().day != self.current_date.day:
                # date have changed, uypload and restart now,
                self.current_date = datetime.datetime.now()
                self.upload_file()
                self.generate_new_object_name()
                self.start_file()
            return True

    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_error disconnects the stream
            return False


if __name__ == '__main__':

    # This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    # pass in our own listerner 
    stream = Stream(auth, l)
    try:
        stream.filter(track=['COVID19'])
    except:
        print('sleep for 10 min')
        time.sleep(60*10)
