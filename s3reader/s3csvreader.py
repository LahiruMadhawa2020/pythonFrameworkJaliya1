import boto3
import json
import logging
import botocore.exceptions


class CsvReader:
    s3Bucket = boto3.resource('s3')

    def read_csv(self, becket, filename):

        bucket = self.s3Bucket.Bucket(becket)
        logging.info('reading files from S3 - csv format')
        full_csv_string_content = None
        file_count = 1
        total_csv_data = ""
        file_path = lambda filename: filename+"/" if filename != "" else ""
        try:
            for obj in bucket.objects.filter(Prefix=file_path(filename)):
                print('{0}:{1}'.format(bucket.name, obj.key))

                file_content = obj.get()['Body'].read().decode('utf-8')  # file content reading and performing decoding

                if file_count == 1:
                    full_csv_string_content = file_content
                else:
                    full_csv_string_content = full_csv_string_content + "\n"+file_content  # concatenation of CSV content
                file_count = file_count + 1

            # print(full_json_string_content)
            # print("--af---")
            total_csv_data = full_csv_string_content

            # dfs.append(cc1)

        except (Exception, botocore.exceptions.ClientError) as err:
            logging.error("Received error: %s", err, exc_info=True)
        print(total_csv_data)
        return total_csv_data
