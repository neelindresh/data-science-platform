import paramiko
import sys
import mlflowlib

class S3Bucket():
    def __init__(self):
        self.expName = 'test-run3'
        self.fileName = 'dataSet1.parquet'
        self.fileVersion = '1.3'

    def connect(self,host,username,key):
        '''
        CONNECTING TO REMOTE SERVER
        :param host: host address
        :param username: AWS user name
        :param key: PATH to the .pem file
        :return: NONE
        '''
        self.host=host
        self.username=username
        print('Connecting to host ',host)
        try:
            key=key
            k = paramiko.RSAKey.from_private_key_file(key)
            self.session = paramiko.SSHClient()
            self.session.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            try:
                self.session.connect(hostname=host, username=username, pkey=k)
            except Exception as e:
                print(e)
                print('Please Check the HOST NAME OR USER NAME')
                sys.exit(0)
        except Exception as e:
            print(e)
            print('Cannot be connected')
            sys.exit(0)
        print('Connected')


    def get_file_list(self,options=None):
        '''
        GIVE US THE LIST OF FILES IN REMOTE LOCATION
        :param options: [LIST] of parameters -a ,-l <Optional>
        :return:[LIST] of files
        '''
        command='ls'
        if options is not None:
            for option in options:
                command+=option
        stdin_, stdout_, stderr_ = self.session.exec_command("ls")
        # time.sleep(2)    # Previously, I had to sleep for some time.
        stdout_.channel.recv_exit_status()
        if len(stderr_.readlines()) is 0:
            lines=stdout_.readlines()
            return lines
        else:
            print(stderr_.readlines())
            self.close()
            sys.exit(0)

    def get_data_remote(self,local,remote):
        '''
        SAVES DATA FROM REMOTE SERVER
        :param local: LOCAL file name
        :param remote: Remote file name
        :return: NONE
        '''
        sftp = self.session.open_sftp()
        print('Getting data...')
        try:
            localpath = local
            remotepath = remote
            sftp.get(localpath, remotepath)
            print('File saved to local')
        except Exception as e:
            print(e)
            print('FILE NOT FOUND')

        sftp.close()
        
    def save_data_remote(self,local,remote):
        '''
        SAVES DATA TO REMOTE SERVER
        :param local: LOCAL file name
        :param remote: Remote file name
        :return: NONE
        '''
        print('Saving data...')
        sftp = self.session.open_sftp()
        try:
            localpath = local
            remotepath = remote
            sftp.put(localpath, remotepath)
            self.fileName=remote.split('/')[-1]
            print('Data saved remotely to ',self.host,' as ', self.create_URL())
        except Exception as e:
            print(e)
            print('FILE NOT FOUND')

        sftp.close()
    def create_URL(self):
        self.fileName=self.host+'/'+self.fileName
        return self.fileName
    def close(self):
        '''
        Closes the session
        :return:
        '''
        filename=self.create_URL()
        mlflowlib.log_file(self.expName, self.fileVersion, filename)
        print('Closing Session')
        self.session.close()
        print('Session Ended')
    def save_dir_remote(self,local,remote):
        '''
        SAVES Directory TO REMOTE SERVER
        :param local: LOCAL file name
        :param remote: Remote file name
        :return: NONE
        '''
        import os
        pwd=os.getcwd()
        try:
            list_of_remote_dir=self.get_file_list()
            list_of_remote_dir=[l.strip('\n') for l in list_of_remote_dir]
            if remote.split('/')[-1] not in list_of_remote_dir:
                print('not there',remote.split('/')[-1])
                self.session.exec_command(str('mkdir '+str(remote.split('/')[-1])))
            
            sftp = self.session.open_sftp()
            print('Saving data...')    
            sftp.chdir(str(remote.split('/')[-1]))
            os.chdir(local)
            for file in os.listdir():
                if file != '.ipynb_checkpoints':
                    print('Saving File-->',file)
                    sftp.put(file,file)
        except Exception as e:
            print(e)
        finally:
            os.chdir(pwd)
            sftp.close()
    def get_dir_remote(self,local,remote):
        '''
        Gets Directory from REMOTE SERVER
        :param local: LOCAL file name
        :param remote: Remote file name
        :return: NONE
        '''
        import os
        pwd=os.getcwd()
        try:
            local_dir=os.listdir()
            
            if local.split('/')[-1] not in local_dir:
                os.mkdir(local.split('/')[-1])
            
            sftp = self.session.open_sftp()
            sftp.chdir(str(remote.split('/')[-1]))
            os.chdir(local)
            print('Getting data...')
            stdin_, stdout_, stderr_ = self.session.exec_command("cd "+str(remote.split('/')[-1])+" \n ls")
            list_of_remote=stdout_.readlines()
            list_of_remote=[l.strip('\n') for l in list_of_remote]
            for file in list_of_remote:
                print('Getting File-->',file)
                sftp.get(file,file)
            print(list_of_remote)
        except Exception as e:
            print(e)
        finally:
            os.chdir(pwd)
            sftp.close()
            