from boto3 import client as boto3_client
import face_recognition
import pickle
import json
import urllib.parse
import os, csv


input_bucket = "Enter your input S3 bucket name here"
output_bucket = "Enter your output S3 bucket name here"
accessKey = str(os.getenv("accessKey"))
secretKey = str(os.getenv("secretKey"))
REGION_NAME = "us-east-1"
s3_client = boto3_client('s3', aws_access_key_id=accessKey, aws_secret_access_key=secretKey, region_name=REGION_NAME)
db_client = boto3_client('dynamodb', aws_access_key_id=accessKey, aws_secret_access_key=secretKey, region_name=REGION_NAME)


# Function to read the 'encoding' file
def open_encoding(filename):
	file = open(filename, "rb")
	data = pickle.load(file)
	file.close()
	return data


# Lambda function to run the face recognition functionality 
def face_recognition_handler(event, context):	
	print("Inside face_recognition_handler")
	### get the latest file from the S3 bucket into the tmp folder
	try:
		bucket = event['Records'][0]['s3']['bucket']['name']
		input_file_name = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
		filepath = "/tmp/"+input_file_name
		s3_client.download_file(input_bucket, input_file_name, filepath)
		
	except Exception as e:
		print("Cannot get the input file")
		print(e)
		print()

    ### Extract a frame from the video
	os.system("ffmpeg -skip_frame nokey -i "+str(filepath)+"  -vsync 0 -frame_pts true /tmp/img%d.jpeg")

    ### perform face_recognition on the frame
	ec_file = os.path.basename("encoding")
	known_encoding = open_encoding(ec_file)
	unknown_image = face_recognition.load_image_file("/tmp/img0.jpeg")
	unknown_encoding = face_recognition.face_encodings(unknown_image)[0]
	result = face_recognition.compare_faces(known_encoding['encoding'], unknown_encoding)
	recognised = str(known_encoding['name'][result.index(True)])

	print("recognised : " + recognised)
	### Get the db response related to the person in te image
	data = db_client.get_item(
      TableName = 'Enter your DynamoDB table name here(Keep name as the partition key',
      Key = {
          'name': {'S': recognised}
    })
	print("data: ", data)

	### upload the response to S3
	x = input_file_name.split('.')[0]+'.csv'
	res_major = data['Item']['major']['S']
	res_year = data['Item']['year']['S']
	res_header = ['name','major','year']
	res_data = [recognised,res_major, res_year]
	with open("/tmp/"+x, 'w', encoding='UTF8') as f:
		writer = csv.writer(f)
		# write the header
		writer.writerow(res_header)
		# write the data
		writer.writerow(res_data)
	object_name = os.path.basename(x)
	response = s3_client.upload_file("/tmp/"+x, output_bucket, object_name)

	print("response: ", response)
	### delete the file created and downloaded 
	c1 = "rm -rf /tmp/"+input_file_name
	os.system(c1)
	c1 = "rm -rf /tmp/"+x
	os.system(c1)
	os.system("rm -rf /tmp/img0.jpeg")

	print("------ Done ------")