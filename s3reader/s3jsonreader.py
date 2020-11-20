import boto3
import json
import logging
import botocore.exceptions


class JsonReader:

    s3Bucket = boto3.resource('s3')

    def read_json(self, becket, filename):

        dfs = []
        bucket = self.s3Bucket.Bucket(becket)
        logging.info('reading files from S3')
        full_json_string_content = "{}"
        file_count = 1
        try:
            for obj in bucket.objects.filter(Prefix=filename + '/'):
                print('{0}:{1}'.format(bucket.name, obj.key))

                file_content = obj.get()['Body'].read().decode('utf-8')  # file content reading and performing decoding
                new_line_removal_stage_1 = file_content.replace('}\n\n{', '},{')  # remove any double new lines in the JSON file
                new_line_removal_stage_2 = new_line_removal_stage_1.replace('}\n{', '},{')  # remove any single new line in the JSON
                if file_count == 1:
                    full_json_string_content = new_line_removal_stage_2
                else:
                    full_json_string_content = full_json_string_content+","+new_line_removal_stage_2  # concaternation of JSON content
                file_count = file_count+1

            #print(full_json_string_content)
            #print("--af---")
            json_data = json.loads(f'[{full_json_string_content}]')


            #dfs.append(cc1)

        except (Exception, botocore.exceptions.ClientError) as err:
            logging.error("Received error: %s", err, exc_info=True)

        return json_data

    def test(self):
        pass
            # for obj in bucket.objects.filter(Prefix=filename + '/'):
            #     print('{0}:{1}'.format(bucket.name, obj.key))
            #     file_content = obj.get()['Body'].read().decode('utf-8')
            #     try:
            #         json_content = json.loads(file_content)
            #     except:
            #         print("error")
            #     dfs.append(json_content)
            # return dfs