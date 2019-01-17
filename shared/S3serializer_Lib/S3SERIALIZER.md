# S3Serializer

S3Bucket class uses the following functions:

1.__init__
⦁	The init constructor is the default function which gets the expname, filename and fileversion as input.

2.  connect
⦁	The connect function is used to get host,username and key as input.
⦁	It gets the RSA key from SSHClient and accept policy to connect session in mlflowlib.
⦁	If hostname,user and key is given correct it connect the session.
⦁	Else, it provides the exception and exit.

3.  get_file_list
⦁	The get file list is used to give the list of files in the remote location.
⦁	It gets the command ls to list and read filelist.

4.  get_data_remote
⦁	The get data remote function is used to  get local and remote as input.
⦁	The session is open to process the local and remote path files.
⦁	It uses get function, to save the file in local. Else it returns exception.

5.  save_data_remote
⦁	The save data remote function is used to  get local and remote as input.
⦁	The session is open to process the local and remote path files.
⦁	It uses put function, to save the file in remote. Else it returns exception.

6.  create_URL
⦁	The create url function is used to create the url based on the filename and the host name.

7.  close
⦁	The close function is used to close the session of mlflowlib.
⦁	It uses close function to end the session.

8.  save_dir_remote
⦁	The save dir remote function is used to save the directory to the remote server.
⦁	It gets the cwd from the list of remote directory and split the files.
⦁	It  opens the session to save the directory. If file is available , it saves data and make directory. Else,  it returns exception and close session.

9.  get_dir_remote
⦁	The get  dir remote function is used to get the directory to the remote server.
⦁	It gets local and remote file  as input.
⦁	It gets the cwd from the list of remote directory and split the files.
⦁	It  opens the session to process and read data from the directory and returns list. Else,  it returns exception and close session.
