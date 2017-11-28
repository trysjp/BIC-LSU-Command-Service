from __future__ import print_function

import xmlrpclib
import ssl
import time

import argparse

import json


if __name__ == "__main__":

    # python BICLSU_PortalAgent.py --url "https://localhost:8000/BIC-LSU_Command_Service/threaded" --call 
    
    # "{\"effective_user\":\"cchiu1\",\"command_target\":\"path\",\"command_operation\":\"list\",\"arguments\":{\"paths\":[]}}"
    # "{\"effective_user\":\"cchiu1\",\"command_target\":\"path\",\"command_operation\":\"list\",\"arguments\":{\"paths\":[\"\/0\/1\/2\"]}}"
    # "{\"effective_user\":\"cchiu1\",\"command_target\":\"path\",\"command_operation\":\"copy\",\"arguments\":{\"paths\":[\"\/0\/1\/2\/src_path\", \"\/0\/1\/2\/dst_path\"]}}"
    # "{\"effective_user\":\"cchiu1\",\"command_target\":\"path\",\"command_operation\":\"copy\",\"arguments\":{\"paths\":[\"\/0\/1\/7\/src_path\", \"\/0\/1\/2\/dst_path\/\"]}}"
    # "{\"effective_user\":\"cchiu1\",\"command_target\":\"path\",\"command_operation\":\"create\",\"arguments\":{\"paths\":[\"\/0\/1\/2\/new_path\"]}}"
    # "{\"effective_user\":\"cchiu1\",\"command_target\":\"path\",\"command_operation\":\"create\",\"arguments\":{\"paths\":[\"\/0\/1\/2\/new\\\"double quoted\\\"path\"]}}"
    # "{\"effective_user\":\"cchiu1\",\"command_target\":\"path\",\"command_operation\":\"create\",\"arguments\":{\"paths\":[\"\/0\/1\/2\/new'single quoted'path\"]}}"
    # "{\"effective_user\":\"cchiu1\",\"command_target\":\"path\",\"command_operation\":\"rename\",\"arguments\":{\"paths\":[\"\/0\/1\/2\/old_path\", \"\/0\/1\/2\/new_path\"]}}"

    parser = argparse.ArgumentParser()
    parser.add_argument('--url', required=True)
    parser.add_argument('--call', required=True)
    args = parser.parse_args()
    
    rpc_server = \
        xmlrpclib.ServerProxy(
            args.url, allow_none=True, 
            context=ssl._create_unverified_context())
            
    try:

        return_value = \
            rpc_server.launch(args.call)
        # {"status":<status>, "result":<result>}
    
    except xmlrpclib.ProtocolError as err:
        print("A protocol error occurred")
        print("URL: " + str(err.url))
        print("HTTP/HTTPS headers: " + str(err.headers))
        print("Error code: " + str(err.errcode))
        print("Error message: " + str(err.errmsg))
    
    except xmlrpclib.Fault as err:
        print("A fault occurred")
        print("Fault code: " + str(err.faultCode))
        print("Fault string: " + str(err.faultString))

    else:

        print(str(return_value))

# end of __main__
