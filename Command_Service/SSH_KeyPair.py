from __future__ import print_function

from Authentication import *

import paramiko

class SSH_KeyPair(Authentication):

    def __init__(self):
    
        #super(self.__class__, self).__init__()
        super(SSH_KeyPair, self).__init__()
        
        print("create object for SSH_KeyPair authentication")
    
    # end of function __init__
    
    def __del__(self):
    
        print("clean SSH_KeyPair")
    
    # end of function __del__
    
    ##
    # @brief    Generate an SSH Key Pair.
    #
    # @param    algorithm   Encryption algorithm to use.
    # @param    key_length  Key length.
    # @param    prefix      Prefix of the key pair.
    # @param    path        Relative path of the key pair.
    #
    # @return   True if succeeded.  Otherwise, False.
    #
    def generateControlCredential(self, 
        algorithm, key_length, 
        prefix, path,
        comment = None, passphrase = None, launch = True):
    
        if not launch:
            print("algo: " + str(algorithm))
            print("len: " + str(key_length))
            print("pre: " + str(prefix))
            print("path: " + str(path))
            print("cmt: " + str(comment))
            print("pass: " + str(passphrase))
            
            return (True, "dry run")
    
        if key_length < 2048:
            key_length = 2048
    
        try:
            if algorithm == "rsa":
                private_key = paramiko.rsakey.RSAKey.generate(key_length)
            elif algorithm == "dsa":
                private_key = paramiko.ecdskey.DSSKey.generate(key_length)
            else:
                raise paramiko.SSHException("Unsupported encryption algorithm.")
                
            private_key.write_private_key_file(
                prefix + path, password = passphrase)
            public_key = private_key.get_base64()
            public_key_marker = private_key.get_name()
            public_key = public_key_marker + " " + public_key
            with open(prefix + path + ".pub", "w") as file:
                file.write(public_key)
                if comment:
                    file.write(" " + str(comment))
        except paramiko.SSHException as e:
            print("Exception in generateControlCredential: " + e.strerror)
            return (False, "failed")
    
        return (True, public_key)
    
    # end of function generateControlCredential 
    #(implementation of the abstract function)

# end of class SSH_KeyPair

if __name__ == "__main__":

    ssh_key_pair = SSH_KeyPair()
    if ssh_key_pair.generateControlCredential(
        "rsa", 3072,
        "./", "testKeyPair", launch=True):
        print("succeeded.")
    else:
        print("failed.")

# end of __main__
