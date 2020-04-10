install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt

run:
	python3 twitter_downloader_to_S3_boto3.py