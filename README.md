# Twitter_streaming_analysis_pyspark
Download twitter streaming with EC2(Spot instance), analysis with EMR PySpark/ SageMaker
![result](https://user-images.githubusercontent.com/8799320/80146403-c9ba6c00-857f-11ea-9609-cd668ba7c93b.png)

## On the spot instance:

git clone this repository

    git clone https://github.com/Superpowergalaxy/Twitter_streaming_analysis_pyspark

Install necessary libraires

    make install
    make run

## On Sagemaker
Open an SageMaker creare a notebook instance with 
    instance type = ml.t2.2xlarge or above

Add S3 access to the defualt IAM role

Add this Git repository

start the 'twitter analaysis pyspark sagemaker.ipynb' notebook 


