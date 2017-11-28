from __future__ import print_function

import paramiko
import select

from Remote_Session import *

class SSH_Session(Remote_Session):

    def __init__(self,
        hostname,
        port,
        username,
        key_filename = None,
        timeout = 10,
        password = None):
    
        super(self.__class__, self).__init__()
        
        if key_filename:
            self.SSH_session = paramiko.SSHClient()
            self.SSH_session.load_system_host_keys()
            self.SSH_session.set_missing_host_key_policy( \
                paramiko.WarningPolicy())
            self.SSH_session.connect( \
                    hostname = hostname, port = port, \
                    username = username, password = password, \
                    key_filename = key_filename, \
                    timeout = timeout, allow_agent = False, compress = True)
        elif not key_filename and password:
            self.SSH_session = paramiko.SSHClient()
            self.SSH_session.load_system_host_keys()
            self.SSH_session.set_missing_host_key_policy( \
                paramiko.WarningPolicy())
            self.SSH_session.connect( \
                    hostname = hostname, port = port, \
                    username = username, password = password, \
                    timeout = timeout, allow_agent = False, compress = True)
        
        print("init SSH_Session")
    
    # end of function __init__
    
    def executeCommandLine(self, command):
    
        #print("exe: " + str(command))
        
        return_string = {}
    
        try:
            stdin,stdout,stderr = \
                self.SSH_session.exec_command(str(command))
        except paramiko.SSHException as e:
        
            print("SSH session exception: " + str(e.message))
            return_value["status"] = False
            return_value["message"] = str(e.message)
            
        else:
   
            return_string[stdout.channel] = ""
            return_string[stderr.channel] = ""
            
            read_test_list = [stdout.channel, stderr.channel]
            
            while True:
            
                readable_list,writable_list,exception_list = \
                    select.select(read_test_list, [], [], 3.0)
                    
                for one_readable_channel in readable_list:
                    
                    string_read = str(one_readable_channel.recv(1024))
                    if string_read:
                        return_string[one_readable_channel] += string_read
                    else:
                        read_test_list.remove(one_readable_channel)
                        
                if not read_test_list:
                    break
            
            # end of while
            
            exit_status = stdout.channel.recv_exit_status()
            
            return_value = {}
            
            return_value["status"] = True
            return_value["exit status"] = exit_status
            return_value["stdout"] = str(return_string[stdout.channel])
            return_value["stderr"] = str(return_string[stderr.channel])
            
            #print("exit status: " + str(return_value["exit status"]))
            #print("stdout: " + str(return_value["stdout"]))
            #print("stderr: " + str(return_value["stderr"]))
    
        return return_value
    
    # end of function executeCommandLine
    
    def transferSmallFile(self, local_path, remote_path, send, launch = True):
    
        return_value = {}
    
        try:
            sftp_session = self.SSH_session.open_sftp()
        except paramiko.SSHException as e:
        
            print("SFTP session opening exception: " + str(e.message))
            return_value["status"] = False
            return_value["message"] = str(e.message)
            return return_value
        
        try:
            # to avoid writing to <remote path>^J instead of <remote path>
            #print("old rp len: " + str(len(remote_path)))
            #remote_path_segments = remote_path.split('/')
            #remote_parent_dir_path = \
            #    "/".join(remote_path_segments\
            #    [0:len(remote_path_segments) - 1])
            #file_name = remote_path_segments[len(remote_path_segments) - 1]
            #file_name_list = sftp_session.listdir(remote_parent_dir_path)
            #for one_name in file_name_list:
            #    if one_name[:len(file_name)] == file_name:
            #        remote_path = remote_parent_dir_path + "/" + one_name
            #        break
            #print("dst rp len: " + str(len(remote_path)))
            
            if send:
                if launch:
                    sftp_session.put(local_path, remote_path, confirm=True)
                else:
                    print("loc: " + str(local_path) + \
                        " to rem: " + str(remote_path))
            else:
                if launch:
                    sftp_session.get(remote_path, local_path)
                else:
                    print("rem: " + str(remote_path) + \
                        " to loc: " + str(local_path))
        except paramiko.SSHException as e:
        
            print("SFTP session transferring exception: " + str(e.message))
            return_value["status"] = False
            return_value["message"] = str(e.message)
            return return_value
            
        sftp_session.close()
        
        return_value["status"] = True
    
        return return_value
    
    # end of function transferSmallFile
    
    def __del__(self):
    
        self.SSH_session.close()
    
        print("clean SSH_Session")
    
    # end of function __del__

# end of class SSH_Session

if __name__ == "__main__":

    obj = SSH_Session()
    
    print("unit test: " + type(obj).__name__)
    
# end of function __main__
