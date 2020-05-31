# S3FileMerger
A tool written in Java for merging AWS S3 files efficiently.

## Description ##
This tool provides an efficient way of merging files in AWS S3. For more information 
on how this tool is implemented refer [this](https://medium.com/@darshanprabhune31/how-to-merge-files-in-aws-s3-efficiently-using-java-sdk-7e4f294c9b61?source=friends_link&sk=26432efef48d4e387acbc55dc37a50d2) post.

## Install ##
- This Maven project is not handled by the Maven central repository. Thus the user needs
to install it manually in his/her project. 
- Clone this repo in your system using command:
    - `git clone https://github.com/darshan3105/S3FileMerger.git`
- Add this library as a dependency in your Maven project. Refer 
[this](https://devcenter.heroku.com/articles/local-maven-dependencies)
for  information about how to add a library in your Maven project.

## Run ##
You can use this tool in the following way:
- create an object of class [S3FileMergingRequest](https://github.com/darshan3105/S3FileMerger/blob/master/src/main/java/com/github/darshan3105/models/S3FileMergingRequest.java)
- call the S3FileMerger:
    - `S3FileMerger.mergeFiles(s3FileMergingRequest)`
- Note: this tool uses the default AmazonS3 client. So the user must make sure that the environment in 
which this code is deployed grants the permission to make S3 API calls,
