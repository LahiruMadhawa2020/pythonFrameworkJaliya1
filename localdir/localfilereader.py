"""
 This is class to read JSON file from local file system
"""
import os
import json


class LocalFileReader:

    """
    Function to read JSON files
    param1 - full file path e.g. /home/jssu/question
    """
    def read_json(self,folderpath):

        # List all files in a directory using scandir()
        basepath = folderpath
        dfs = []

        with os.scandir(basepath) as entries:
            for entry in entries:
                if entry.is_file():
                    print(entry.name)
                    with open("{}/{}".format(folderpath, entry.name)) as f:
                        body = json.loads(f.read(), encoding='utf-8')
                        dfs.append(body)
        return dfs

    """
        Function to read JSON files
        param1 - full file path e.g. /home/jssu/question
        """

    def read_csv(self, folderpath):

        # List all files in a directory using scandir()
        basepath = folderpath
        dfs = []
        count = 1
        file_content = ""
        with os.scandir(basepath) as entries:
            for entry in entries:
                if entry.is_file():
                    print(entry.name)
                    with open("{}/{}".format(folderpath, entry.name)) as f:
                        content = f.read()
                        if count == 1:
                            file_content = content
                        else:
                            file_content = file_content + "\n" + content
                        count = count+1
        return file_content
