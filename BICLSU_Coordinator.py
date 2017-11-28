from __future__ import print_function

from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler
from SocketServer import TCPServer
from SimpleXMLRPCServer import SimpleXMLRPCDispatcher

import argparse
import xmlrpclib
import json
import ssl

CERT_FILE = './cert.pem'

class SSLServer(TCPServer):
    def get_request(self):
        newsocket, fromaddr = self.socket.accept()
        connstream = ssl.wrap_socket(newsocket, server_side=True,
                                     certfile=CERT_FILE, keyfile=CERT_FILE,
                                     ssl_version=ssl.PROTOCOL_SSLv23)

        return (connstream, fromaddr)

# end of class SSLServer

class threadedSimpleXMLRPCServer(SSLServer,
                         SimpleXMLRPCDispatcher):

    allow_reuse_address = True

    # Warning: this is for debugging purposes only! Never set this to True in
    # production code, as will be sending out sensitive information (exception
    # and stack trace details) when exceptions are raised inside
    # SimpleXMLRPCRequestHandler.do_POST
    _send_traceback_header = True

    def __init__(self, \
        addr, \
        requestHandler=SimpleXMLRPCRequestHandler, \
        logRequests=True, \
        allow_none=True, \
        encoding=None, \
        bind_and_activate=True):
        
        self.logRequests = logRequests

        SimpleXMLRPCDispatcher.__init__(self, allow_none, encoding)
        SSLServer.__init__(self, addr, requestHandler, bind_and_activate)

# end of class threadSimpleXMLRPCServer

# Restrict to a particular path.
class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/BIC-LSU_Command_Service/threaded',)

    def __init__(self, request, client_address, server):
        IP_address, port = client_address
        
        #trusted_IP_address_list = ["127.0.0.1"]
        
        #if IP_address in trusted_IP_address_list:
        #    SimpleXMLRPCRequestHandler.__init__(\
        #        self, request, client_address, server)
        #else:
        #    print("UNTRUSTED IP ADDRESS: \"" + IP_address + "\"")
        SimpleXMLRPCRequestHandler.__init__(\
            self, request, client_address, server)
            
# end of class RequestHandler

class BICLSU_Coordinator(object):

    def __init__(self):
    
        #super(self.__class__, self).__init__()
        super(BICLSU_Coordinator, self).__init__()
        
        # create a table to keep track of all Task IDs
        
        print("init BICLSU_Coordinator")
    
    # end of function __init__
    
    def __del__(self):
    
        print("clean BICLSU_Coordinator")
    
    # end of function __del__
    
    def launch(self, json_call):
    
    # end of function launch

# end of class BICLSU_Coordinator

if __name__ == "__main__":

    # python BICLSU_Command_Service.py --host "localhost" --port 8000 --db-path ":memory:" --ssh-key-pair "/home/chiuchuihui/Works/BICLSU/Authentication/ControlServerSSHCertificate/" "id_rsa_LSU_BTR_Ctl_3072" "id_rsa_LSU_BTR_Ctl_3072.pub" --ssh-cert "/home/chiuchuihui/Works/BICLSU/Authentication/ControlServerSSHCertificate/id_rsa_LSU_BTR_CA_4096.pub" "/home/chiuchuihui/Works/BICLSU/Authentication/ControlServerSSHCertificate/id_rsa_LSU_BTR_Ctl_3072-cert.pub"

    parser = argparse.ArgumentParser()
    parser.add_argument('--host', dest='IP_or_FQDN', required=True)
    parser.add_argument('--port', dest='port', type=int, required=True)
    parser.add_argument('--db-path', dest='DB_path', required=True)
    parser.add_argument('--ssh-key-pair', dest='SSH_key_pair_info', nargs=3, 
        help="<prefix> <pri> <pub>")
    parser.add_argument('--ssh-cert', dest='SSH_cert_info', nargs=2,
        help="<CA pub> <cert>")
    parser.add_argument('--log-prefix', dest='log_path_prefix')
    args = parser.parse_args()

    # Create server
    server = \
        threadedSimpleXMLRPCServer( 
            (args.IP_or_FQDN, args.port), 
            requestHandler=RequestHandler, 
            allow_none=True
        )
    
    log_path_prefix = args.log_path_prefix
    SSH_key_pair_info = args.SSH_key_pair_info
    SSH_cert_info = args.SSH_cert_info
   
    server.register_function(
        BICLSU_coordinator(
            args.DB_path,
            log_path_prefix,
            SSH_key_pair_info,
            SSH_cert_info
        ).launch, 'launch')
    
    # Run the server's main loop
    server.serve_forever()
