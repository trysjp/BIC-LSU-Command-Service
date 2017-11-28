from __future__ import print_function

import subprocess

from SSH_Certificate import *

class OpenSSH_Certificate(SSH_Certificate):

    def __init__(self):
    
        #super(self.__class__, self).__init__()
        super(OpenSSH_Certificate, self).__init__()

        print("init OpenSSH_Certificate")
    
    # end of function __init__
    
    def generateControlCredential(self, 
        prefix, user_key_path, CA_user_key_path, 
        certificate_identity, principals, validity_interval, 
        launch=True):
        
        return_value = {}
        
        if not launch:
            print("pre: " + str(prefix))
            print("usr k: " + str(user_key_path))
            print("CA k: " + str(CA_user_key_path))
            print("cert Id: " + str(certificate_identity))
            print("usr principals: " + str(principals))
            print("interval: " + str(validity_interval))
        else:
            try:
                output = \
                    subprocess.check_output(
                        ["ssh-keygen", 
                        "-s", prefix + CA_user_key_path, 
                        "-I", certificate_identity, 
                        "-n", principals, 
                        "-V", validity_interval, 
                        prefix + user_key_path])
                #retrieve the certificate file name
                certificate_path = output.split(" ")[3][:-1]
                
                return_value = \
                    {"status": True,
                    "path": certificate_path}
                
            except CalledProcessError as e:
                return_value = \
                    {"status": True,
                    "return code": e.returncode,
                    "output": e.output}
        
        return return_value
    
    # end of function generateControlCredential
    #(implementation of the abstract function)

# end of class OpenSSH_Certificate

if __name__ == "__main__":

    obj = OpenSSH_Certificate()
    
    print("unit test: " + type(obj).__name__)
    
# end of function __main__
