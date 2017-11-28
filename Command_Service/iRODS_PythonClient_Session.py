from __future__ import print_function

from irods.session import iRODSSession
from Remote_Session import *

class iRODS_PythonClient_Session(Remote_Session):

    def __init__(self,
        host,
        port,
        user,
        password,
        zone,
        client_user = None,
        client_zone = None):
    
        #super(self.__class__, self).__init__()
        super(iRODS_PythonClient_Session, self).__init__()
    
        if not client_user:
            self.session = \
                iRODSSession(
                    host = host, port = port, 
                    user = user, password = password, 
                    zone = zone)
        else:
            self.session = \
                iRODSSession(
                    host = host, port = port, 
                    user = user, password = password, 
                    zone = zone,
                    client_user = client_user, client_zone = client_zone)
    
        print("init iRODS_PythonClient_Session")
    
    # end of function __init__
    
    def __del__(self):
    
        self.session.cleanup()
    
        print("clean iRODS_PythonClient_Session")
    
    # end of function __del__
    
    def getSession(self):
    
        return self.session
    
    # end of function getSession

# end of class iRODS_PythonClient_Session

if __name__ == "__main__":

    obj = iRODS_PythonClient_Session()
    
    print("unit test: " + type(obj).__name__)
    
# end of function __main__
