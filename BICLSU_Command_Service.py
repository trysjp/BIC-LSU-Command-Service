from __future__ import print_function

from Command_Service import *
from BICLSU_DB_Operator import *
from BICLSU_Partner_System import *

from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler
from SocketServer import ThreadingMixIn
from SocketServer import TCPServer
from SimpleXMLRPCServer import SimpleXMLRPCDispatcher

import argparse
import time
import random
import xmlrpclib
import json
import ssl
import socket
import thread
import os
import subprocess
import tempfile
import traceback

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

class call_exception(Exception):
    pass 
# end of class call_exception

class call_exception_no_target(call_exception):
    pass    
# end of class call_exception_no_target

class call_exception_no_operation(call_exception):
    pass   
# end of class call_exception_no_operation

class parse_exception(Exception):
    pass    
# end of class parse_exception

class parse_exception_wrong_no_arguments(parse_exception):
    pass  
# end of class parse_exception_wrong_no_arguments

class parse_exception_wrong_no_paths(parse_exception):  
    pass
# end of class parse_exception_wrong_no_paths
    
class parse_exception_insufficient_permissions(parse_exception):
    pass
# end of class parse_exception_insufficient_permissions

class parse_exception_not_implemented(parse_exception): 
    pass
# end of class parse_exception_not_implemented

class parse_exception_not_supported(parse_exception): 
    pass
# end of class parse_exception_not_supported

class session_exception(Exception):
    pass
# end of class session_exception

class session_exception_no_matched_bandwidth(session_exception):
    pass
# end of class session_exception_no_matched_IP_version

class session_exception_no_matched_IP_version(session_exception):
    pass
# end of class session_exception_no_matched_IP_version

class BICLSU_Command_Service(object):

    def __init__(self, 
        DB_path,
        coordinator_url, 
        transfer_admin_resc_path,
        iRODS_admin_resc_path,
        portal_admin_resc_path,
        job_script_parsing,
        log_file_prefix, 
        SSH_key_pair_info,
        SSH_cert_info):
    
        #print("init BICLSU_Command_Service")
        
        if job_script_parsing:
            self.job_script_parsing = {}
            self.job_script_parsing["uploaded script resc path"] = \
                job_script_parsing[0]
            self.job_script_parsing["to skeleton parser path"] = \
                job_script_parsing[1]
            self.job_script_parsing["valid tagged script repo path"] = \
                job_script_parsing[2]
            self.job_script_parsing["converted script repo path"] = \
                job_script_parsing[3]
            self.job_script_parsing["to script parser path"] = \
                job_script_parsing[4]
            self.job_script_parsing["script download resc path"] = \
                job_script_parsing[5]
        if log_file_prefix:
            self.log_file_prefix = str(log_file_prefix)
        if SSH_key_pair_info:
            self.SSH_key_pair_info = {}
            self.SSH_key_pair_info["prefix path"] = SSH_key_pair_info[0]
            self.SSH_key_pair_info["private key path"] = SSH_key_pair_info[1]
            self.SSH_key_pair_info["public key path"] = SSH_key_pair_info[2]
        if SSH_cert_info:
            self.SSH_cert_info = {}
            self.SSH_cert_info["CA public key path"] = SSH_cert_info[0]
            self.SSH_cert_info["prefix path"] = SSH_cert_info[1]
            self.SSH_cert_info["certificate path"] = SSH_cert_info[2]
        
        self.db_operator = BICLSU_DB_Operator(str(DB_path))
        self.db_operator.createDB()
        self.db_operator.initializeDB()
        
        # create client for the coordinator
        #self.coordinator = \
        #    xmlrpclib.ServerProxy(
        #        coordinator_url, allow_none=True, 
        #        context=ssl._create_unverified_context())
        #print("pid: " + str(os.getpid()) + ",tid: " + str(thread.get_ident()))
        
        self.transfer_admin_resc_path = transfer_admin_resc_path
        self.iRODS_admin_resc_path = iRODS_admin_resc_path
        self.portal_admin_resc_path = portal_admin_resc_path
        
        self.index_of_1st_local_path_segment = 4;
    
    # end of function __init__
    
    def _testCall(self, user, command, arguments):
    
        print("user: " + str(user))
        print("cmd: " + str(command))
        print("args: " + str(arguments))
    
    # end of function _testCall
    
    def _hasPermissionsForPaths(self, 
        effective_user, 
        path_privilege_dictionary):
    
        # if BIC-LSU iRODS path
            # verify with Python Client
        # else
            # examine BIC-LSU ACL table
            
        # TEMP IMPLEMENTATION.  Only allows accessing user's own resources.
        user_Ids = self.db_operator.getUserIdOf([effective_user])
        effective_user_Id = user_Ids[effective_user]
        
        hasAllPermissions = True
        for path,privilege in path_privilege_dictionary.iteritems():
        
            path_segments = path.split('/')
            if not path_segments[2] == str(effective_user_Id):
                hasAllPermissions = False
                break
    
        return hasAllPermissions
        
    # end of function _hasPermissionsForPaths
    
    def _hasPermissionsForAnnotations(self, 
        user, 
        path_narration_privilege_dictionary):
    
        # examine BIC-LSU ACL table
    
        return True
        
    # end of function _hasPermissionsForNarrations
    
    ##
    # @brief                                Determines if a user can launch an 
    #                                       application on a host.
    #
    # @param    host_application_bindings   [(<application Id>, <host Id>)]}
    #
    # @return   return_value                {<application Id>:
    #                                       {<host Id>:<T or F>}}
    def _hasPermissionsForApplications(self, 
        user, 
        host_application_binding):
        # if no acl,
        # owner rwx
        # other None
        # if with acl, owner/other-GROUP
        # allowed to add/remove files if parent dir is rw
        # allowed to edit files if file is w
        # allowed to list file if parent dir is r
        # allowed to schedule if resource is x
        
        # TO DO
        # support other-USER ACL
    
        return_value = {}
        
        # TEMP IMPLEMENTATION.
        # Allows launching any supported application on the host.
        application_information = \
            self.db_operator.getApplicationsOf([user])
        for one_binding in host_application_bindings:
           pass
    
        return return_value
    
    # end of function _hasPermissionsForApplications
    
    def launch(self, call_json):
        # factory
        
        call = json.loads(call_json)
        target = call["command_target"]
        operation = call["command_operation"]
        
        print("1st level")
        for key,value in call.iteritems():
            print("key: " + str(key) + ", " + "value: " + str(value))
        
        try:
            if target == "path":
            
                if operation == "list":
                    return self._listPath(**call)
                elif operation == "create":
                    return self._createPath(**call)
                elif operation == "remove":
                    return self._removePath(**call)
                elif operation == "copy":
                    return self._copyPath(**call)
                elif operation == "rename":
                    return self._renamePath(**call)
                elif operation == "upload_pre":
                    #return self._prepareSpaceForUploading(**call)
                    pass
                elif operation == "upload_post":
                    #return self._mergeUploadedData(**call)
                    pass
                elif operation == "download_pre":
                    #return self._prepareSpaceForDownloading(**call)
                    pass
                elif operation == "download_post":
                    #return self._cleanSpaceForDownloading(**call)
                    pass
                else:
                    raise \
                        call_exception_no_operation(
                            "Path operation not supported")
                # end of if operation
                
            elif target == "user":
            
                if operation == "add":
                    return self._addUser(**call)
                else:
                    raise \
                        call_exception_no_operation(
                            "User operation not supported")
                # end of if operation
                
            elif target == "host":
            
                if operation == "list":
                    return self._listHostServiceWithCategoryApplication(**call)
                else:
                    raise \
                        call_exception_no_operation(
                            "Host operation not supported")
                    
                # end of if operation
     
            elif target == "resource":
            
                if operation == "add":
                    return self._addResource(**call)
                elif operation == "list":
                    return self._listResource(**call)
                else:
                    raise \
                        call_exception_no_operation(
                            "Resource operation not supported")
                # end of if operation
                
            elif target == "service":
            
                if operation == "list":
                    return self._listService(**call)
                else:
                    raise \
                        call_exception_no_operation(
                            "Service operation not supported")
                # end of if operation
            
            elif target == "friendship":
            
                if operation == "list":
                    return self._listFriendship(**call)
                else:
                    raise \
                        call_exception_no_operation(
                            "Friendship operation not supported")
                # end of if operation
            
            elif target == "group":
            
                if operation == "add":
                    return self._addGroup(**call)
                elif operation == "edit":
                    return self._editGroup(**call)
                elif operation == "remove":
                    return self._removeGroup(**call)
                elif operation == "list":
                    return self._listGroup(**call)
                else:
                    raise \
                        call_exception_no_operation(
                            "Group operation not supported")
                # end of if operation
            
            elif target == "member":
            
                if operation == "add":
                    return self._addMember(**call)
                elif operation == "remove":
                    return self._removeMember(**call)
                elif operation == "list":
                    return self._listMember(**call)
                else:
                    raise \
                        call_exception_no_operation(
                            "Membership operation not supported")
                # end of if operation
                
            elif target == "annotation":
            
                if operation == "add":
                    return self._addAnnotation(**call)
                elif operation == "edit":
                    return self._editAnnotation(**call)
                elif operation == "remove":
                    return self._removeAnnotation(**call)
                elif operation == "list":
                    return self._listAnnotation(**call)
                else:
                    raise \
                        call_exception_no_operation(
                            "Annotation operation not supported")
                # end of if operation
            
            elif target == "ACL":
            
                if operation == "add":
                    return self._addACL(**call)
                if operation == "edit":
                    return self._editACL(**call)
                elif operation == "remove":
                    return self._removeACL(**call)
                elif operation == "list":
                    return self._listACL(**call)
                else:
                    raise \
                        call_exception_no_operation(
                            "ACL operation not supported")
                # end of if operation
            
            elif target == "application":
            
                if operation == "add":
                    return self._addApplication(**call)
                elif operation == "list":
                    return self._listApplication(**call)
                elif operation == "get":
                    return self._getApplication(**call)
                elif operation == "launch_pre":
                    return self._getGUIOfApplication(**call)
                elif operation == "launch_post":
                    return self._launchApplication(**call)
                elif operation == "monitor":
                    return self._monitorApplication(**call)
                elif operation == "abort":
                    return self._abortApplication(**call)
                else:
                    raise \
                        call_exception_no_operation(
                            "Application operation not supported")
                # end of if operation
            
            else:
                raise \
                    call_exception_no_target(
                        "Operation target not supported")
            
            # end of if target
        except call_exception as e:
            print(str(call))
            print("Exception, " + str(e) + ": " + str(e.message))
            status = False
            result = {}
            return_value = {\
                "status": status,
                "result": result
            }
         
            return json.dumps(return_value)
            
        # end of try
    
    # end of function parseArguments
    
    def _listPath(self, **call):
    
        try:
            # check no. of arguments
            if call["arguments"]["paths"] and \
                not len(call["arguments"]["paths"]) == 1:
                raise parse_exception_wrong_no_paths(
                        "incorrect number of paths")
            
            if not call["arguments"]["paths"]:
                # request to list empty path
                # list default path instead
                user_Ids = \
                    self.db_operator.getUserIdOf([call["effective_user"]])
                current_user_Id = user_Ids[call["effective_user"]]
                paths = \
                    self.db_operator.getDefaultPathOf([current_user_Id])
                for one_path in paths[current_user_Id]:
                    if one_path[0] == "storage":
                        call["arguments"]["paths"].append(one_path[1])
                        break
                #print("def storage path: " + str(call["arguments"]["paths"]))
                
            # check permissions
            if not self._hasPermissionsForPaths(\
                        call["effective_user"], \
                        {call["arguments"]["paths"][0]: "r"}):
                raise parse_exception_insufficient_permissions(\
                        "insufficient permissions")
                        
            #print("logical path list: " + str(call["arguments"]["paths"]))
            
            return_value = {}
            
            # process virtual portion of the logical path
            total_logical_path_segments = \
                str(call["arguments"]["paths"][0]).count('/') - 1
                # subtract the leading one
            #print("total log seg: " + str(total_logical_path_segments))
            
            if total_logical_path_segments == 0:
            
                return_value["type"] = "virtual"
            
                return_value["status"] = False
                return_value["message"] = \
                    "autonomous region feature not yet implemented."
                
                return json.dumps(return_value)
                
            # end of if
                
            if total_logical_path_segments == 1:
            
                return_value["type"] = "virtual"
            
                return json.dumps(return_value)
                
            # end of if
                
            if total_logical_path_segments == 2:
                
                return_value["type"] = "virtual"
                
                logical_to_display_information = \
                    self._prepareToDisplayLogicalPaths(
                        call["arguments"]["paths"])
                
                return_value["path hierarchy"] = \
                    logical_to_display_information\
                        [call["arguments"]["paths"][0]]
                
                user_Ids = \
                    self.db_operator.getUserIdOf([call["effective_user"]])
                current_user_Id = user_Ids[call["effective_user"]]
                        
                private_resources = \
                    self.db_operator.getPrivateStorageResourcesOf(
                        [current_user_Id])
                #print("pri resc: " + str(private_resources))
                
                return_value["entries"] = []
                for one_resource in private_resources[current_user_Id]:
                    one_entry = {}
                    one_entry["type"] = 'd'
                    one_entry["path"] = \
                        str(one_resource["private resource Id"]) + '/'
                    one_entry["display"] = one_resource["display"]
                    return_value["entries"].append(one_entry)
                    
                # list effective user and all readable entries according to ACL
                #/<region>/<user>/other/<other region>/<other user>/<other path>
                result = \
                    self.db_operator.getAllAccessiblePathFromOthers(
                        [{"user": call["effective_user"],
                        "IO_access": 1, #read
                        "computing_access": 0}]) #none
                #print("ACL acc: " + str(result))
                if result:
                    #generate ACL logical paths
                    # TO DO
                    # support autonamous region
                    logical_path_ACL_prefix = \
                        "/0/" + str(current_user_Id) + "/other"
                    logical_paths_ACL = []
                    key = \
                        str((str(call["effective_user"]),
                        int(1), # read I/O access
                        int(0))) # none computing access
                    for one_path in result[key]:
                        logical_paths_ACL.append(logical_path_ACL_prefix + \
                            one_path["resource"] + one_path["relative"])
                    #print("ACL log_p: " + str(logical_path_ACL))
                    result = \
                        self._prepareToDisplayLogicalPaths(logical_paths_ACL)
                    #print("display ACL log_p: " + str(result))
                    for key,value in result.iteritems():
                        one_entry = {}
                        one_entry["type"] = 'd'
                        one_entry["path"] = \
                            str("".join(value[i]["logical"] \
                                for i in range(2, len(value), 1)))
                        one_entry["display"] = \
                            "".join(value[i]["display"] \
                                for i in range(2, len(value), 1))
                        #print("1 entry: " + str(one_entry))
                        return_value["entries"].append(one_entry)
                
                return json.dumps(return_value)
                
            # end of if
            
            return_value["type"] = "physical"
            
            # logical to physical mapping      
            logical_to_physical_information = \
                self._prepareToManipulatePartnerSystems(\
                    call["arguments"]["paths"])
                    
            #print("log to phy info: " + str(logical_to_physical_information))
        
            # launch logical operation
            for logical_path, partner_system \
                in logical_to_physical_information.iteritems():
                    partner_system.setListType(call["arguments"]["list type"])
                    result = \
                        partner_system.launchFileOperation("list")
                    #print("result " + str(result))
                    if result["status"]:
                        # logical to display name mapping
                        logical_to_display_information = \
                            self._prepareToDisplayLogicalPaths([logical_path])
                            
                        #print("log to disp: " + \
                        #    str(logical_to_display_information[logical_path]))
                            
                        return_value = result
                        return_value["path hierarchy"] = \
                            logical_to_display_information[logical_path]
                        return_value = json.dumps(return_value)
            
        except Exception as e:
            traceback.print_exc()
            print("Exception: " + e.message)
            status = False
            result = {}
            # log failure
            #self.log_file_prefix + time + user + command + process
            
        # end of try
         
        return return_value
        
    # end of function _listPath
    
    def _createPath(self, **call):
    
        try:
            # check no. of arguments
            if not len(call["arguments"]["paths"]) == 1:
                raise parse_exception_wrong_no_paths(
                        "incorrect number of paths")
                
            # check permissions
            # extract parent path
            path_segments = call["arguments"]["paths"][0].split('/')
            if call["arguments"]["paths"][0]\
                [len(call["arguments"]["paths"][0]) - 1] == '/':
                parent_path = '/'.join(path_segments[:len(path_segments) - 1])
            else:
                parent_path = '/'.join(path_segments[:len(path_segments)])
            #print("parent: " + str(parent_path))
            
            if not self._hasPermissionsForPaths(\
                        call["effective_user"], \
                        {parent_path: "w"}):
                raise parse_exception_insufficient_permissions(\
                        "insufficient permissions")
                        
            #print("logical path list: " + str(call["arguments"]["paths"]))
            
            # logical to physical mapping      
            logical_to_physical_information = \
                self._prepareToManipulatePartnerSystems(\
                    call["arguments"]["paths"])
                    
            #print("log to phy info: " + str(logical_to_physical_information))
        
            # launch logical operation
            for logical_path, partner_system \
                in logical_to_physical_information.iteritems():
                    result = partner_system.launchFileOperation("create")
                    #print("result " + str(result))
                            
                    return_value = result  
            
        except Exception as e:
            print("Exception: " + e.message)
            status = False
            result = {}
            # log failure
            #self.log_file_prefix + time + user + command + process
            
        # end of try
         
        return json.dumps(return_value)
    
    # end of function _createPath
    
    def _renamePath(self, **call):
    
        try:
            # check no. of arguments
            if not len(call["arguments"]["paths"]) == 2:
                raise parse_exception_wrong_no_paths(
                        "incorrect number of paths")
                
            # check permissions
            # extract parent path
            path_segments = call["arguments"]["paths"][0].split('/')
            if call["arguments"]["paths"][0]\
                [len(call["arguments"]["paths"][0]) - 1] == '/':
                parent_path = '/'.join(path_segments[:len(path_segments) - 1])
            else:
                parent_path = '/'.join(path_segments[:len(path_segments)])
            #print("parent: " + str(parent_path))
            
            if not self._hasPermissionsForPaths(\
                        call["effective_user"], \
                        {parent_path: "w",
                        call["arguments"]["paths"][0]: "w"}):
                raise parse_exception_insufficient_permissions(\
                        "insufficient permissions")
                        
            #print("logical path list: " + str(call["arguments"]["paths"]))
            
            # logical to physical mapping      
            logical_to_physical_information = \
                self._prepareToManipulatePartnerSystems(\
                    call["arguments"]["paths"])
                    
            #print("log to phy info: " + str(logical_to_physical_information))
        
            # launch logical operation
            # logical paths are provided in pairs
            partner_system = \
                logical_to_physical_information[call["arguments"]["paths"][1]]
            local_path_list = \
                partner_system.getLocalPaths()
            partner_system = \
                logical_to_physical_information[call["arguments"]["paths"][0]]
            partner_system.addLocalPaths(local_path_list)
            result = partner_system.launchFileOperation("rename")
            return_value = result
                    
        except Exception as e:
            print("Exception: " + e.message)
            return_value["status"] = False
            # log failure
            #self.log_file_prefix + time + user + command + process
            
        # end of try
        
        return json.dumps(return_value)
    
    # end of function _renamePath
    
    def _removePath(self, **call):
    
        try:
            # check no. of arguments
            if not len(call["arguments"]["paths"]) == 1:
                raise parse_exception_wrong_no_paths(
                        "incorrect number of paths")
                
            # check permissions
            # extract parent path
            path_segments = call["arguments"]["paths"][0].split('/')
            if call["arguments"]["paths"][0]\
                [len(call["arguments"]["paths"][0]) - 1] == '/':
                parent_path = '/'.join(path_segments[:len(path_segments) - 1])
            else:
                parent_path = '/'.join(path_segments[:len(path_segments)])
            #print("parent: " + str(parent_path))
            
            if not self._hasPermissionsForPaths(\
                        call["effective_user"], \
                        {parent_path: "w",
                        call["arguments"]["paths"][0]: "w"}):
                raise parse_exception_insufficient_permissions(\
                        "insufficient permissions")
                        
            #print("logical path list: " + str(call["arguments"]["paths"]))
            
            # logical to physical mapping      
            logical_to_physical_information = \
                self._prepareToManipulatePartnerSystems(\
                    call["arguments"]["paths"])
                    
            #print("log to phy info: " + str(logical_to_physical_information))
        
            # launch logical operation
            for logical_path, partner_system \
                in logical_to_physical_information.iteritems():
                    result = partner_system.launchFileOperation("remove")
                    #print("result " + str(result))
                            
                    return_value = result  
            
        except Exception as e:
            print("Exception: " + e.message)
            status = False
            result = {}
            # log failure
            #self.log_file_prefix + time + user + command + process
            
        # end of try
         
        return json.dumps(return_value)
    
    # end of function _removePath
    
    def _prepareToManipulatePartnerSystems_old(self, logical_paths):
    
        # TO DO
        # support ACL paths
    
        return_value = {}
    
        for one_logical_path in logical_paths:
        
            # logical to host+physical
            # /<autonomus regin>/<user>/<resource>/<local path>
            logical_path_segments = one_logical_path.split('/');
            
            #print("logical path segments: " + str(logical_path_segments))
            
            host_information = \
                self.db_operator.getPartnerSystemInformation(
                    logical_path_segments\
                        [1:self.index_of_1st_local_path_segment])
            if not host_information:
                return None
                
            print("host info: " + str(host_information))
                    
            relative_path = \
                '/'.join(logical_path_segments\
                    [self.index_of_1st_local_path_segment:])
                    
            local_path = \
                str(host_information[unicode("root path")]) + relative_path
        
            return_value[one_logical_path] = \
                PartnerSystem( \
                    {"control SSH key pair information": self.SSH_key_pair_info,
                    "control SSH certificate information": self.SSH_cert_info,
                    "job script parsing information":self.job_script_parsing,
                    "host information": host_information,
                    "relative paths": [relative_path],
                    "local paths": [local_path]
                    }
                )

        # end of for
        
        return return_value
    
    # end of function _prepareToManipulatePartnerSystems
    
    def _prepareToManipulatePartnerSystems(self, logical_paths):
    
        return_value = {}
    
        for one_logical_path in logical_paths:
        
            # logical to host+physical
            # /<autonomus regin>/<user>/<resource>/<local path>
            logical_path_segments = one_logical_path.split('/');
            
            #print("logical path segments: " + str(logical_path_segments))
            
            if logical_path_segments[3] == "other":
                # other users' paths accessible via ACL
            
                #/<region>/<user>/other/
                #<other region>/<other user>/<other resource>/<other local path>
                host_information = \
                    self.db_operator.getPartnerSystemInformation(
                        logical_path_segments\
                            [self.index_of_1st_local_path_segment:\
                            self.index_of_1st_local_path_segment + \
                            self.index_of_1st_local_path_segment - 1])
                if not host_information:
                    return None
                
                print("host info: " + str(host_information))
                    
                relative_path = \
                    '/'.join(logical_path_segments\
                        [self.index_of_1st_local_path_segment + \
                        self.index_of_1st_local_path_segment - 1:])
            
            else:
                # my paths
            
                host_information = \
                    self.db_operator.getPartnerSystemInformation(
                        logical_path_segments\
                            [1:self.index_of_1st_local_path_segment])
                if not host_information:
                    return None
                
                print("host info: " + str(host_information))
                    
                relative_path = \
                    '/'.join(logical_path_segments\
                        [self.index_of_1st_local_path_segment:])
                    
            local_path = \
                str(host_information[unicode("root path")]) + relative_path
        
            return_value[one_logical_path] = \
                PartnerSystem( \
                    {"control SSH key pair information": self.SSH_key_pair_info,
                    "control SSH certificate information": self.SSH_cert_info,
                    "job script parsing information":self.job_script_parsing,
                    "host information": host_information,
                    "relative paths": [relative_path],
                    "local paths": [local_path]
                    }
                )

        # end of for
        
        return return_value
    
    # end of function _prepareToManipulatePartnerSystems
    
    def _prepareToDisplayLogicalPaths_old(self, logical_paths):
        # logical paths for display must end with /
    
        # TO DO
        # support ACL paths
    
        return_value = {}
    
        for one_logical_path in logical_paths:
            
            logical_path_segments = one_logical_path.split('/');
        
            if len(logical_path_segments) <= \
                self.index_of_1st_local_path_segment:
                index_of_last_virtual_path_segment_plus_1 = \
                    len(logical_path_segments) - 1
            else:
                index_of_last_virtual_path_segment_plus_1 = \
                    self.index_of_1st_local_path_segment
            #print("process 1 thru " + str(index_of_last_virtual_path_segment_plus_1 - 1))
            
            result_list = \
                self.db_operator.mapLogicalPathsToDisplayNames(
                    logical_path_segments\
                        [1:index_of_last_virtual_path_segment_plus_1])
                    
            for index in \
                range(self.index_of_1st_local_path_segment, 
                    len(logical_path_segments) - 1, 1):
                
                result_list.append(
                    {"logical": str(logical_path_segments[index]) + '/', 
                    "display": str(logical_path_segments[index])})
            
            return_value[one_logical_path] = result_list
        
        # end of for
    
        return return_value
    
    # end of function _prepareToDisplayLogicalPath
    
    def _prepareToDisplayLogicalPaths(self, logical_paths):
        # logical paths for display must end with /
    
        return_value = {}
    
        for one_logical_path in logical_paths:
            
            logical_path_segments = one_logical_path.split('/');
            
            if len(logical_path_segments) >= 3 and \
                logical_path_segments[3] == "other":
                # others' path accessible via ACL
            
                if len(logical_path_segments) <= \
                    self.index_of_1st_local_path_segment + 3:
                    index_of_last_virtual_path_segment_plus_1 = \
                        len(logical_path_segments) - 1
                else:
                    index_of_last_virtual_path_segment_plus_1 = \
                        self.index_of_1st_local_path_segment + 3
                #print("process 1 thru " + \
                #    str(index_of_last_virtual_path_segment_plus_1 - 1))
                
                result_list = \
                    self.db_operator.mapLogicalPathsToDisplayNames(
                        logical_path_segments[1:3])
                #print(str(result_list))
                            
                result_list_other = \
                    self.db_operator.mapLogicalPathsToDisplayNames(
                        logical_path_segments\
                            [4:index_of_last_virtual_path_segment_plus_1])
                #print(str(result_list_other))

                # combine "other/<region>/<user>/<resource>/"
                result_list_other[0]["logical"] = \
                    "other" + str(result_list_other[0]["logical"])
                result_list_other[0]["display"] = \
                    "Shared from " + str(result_list_other[0]["display"]) + "/"
                    
                for index in range(1, 3, 1):
                    result_list_other[0]["logical"] += \
                        result_list_other[index]["logical"]
                    result_list_other[0]["display"] += \
                        result_list_other[index]["display"] + "/"
                result_list.append(result_list_other[0])
                #print(str(result_list_other))
                        
                for index in \
                    range(index_of_last_virtual_path_segment_plus_1, 
                        len(logical_path_segments) - 1, 1):
                    
                    result_list.append(
                        {"logical": str(logical_path_segments[index]) + '/', 
                        "display": str(logical_path_segments[index])})     
                #print("result ls: " + str(result_list))
            
            else:
                # my path
        
                if len(logical_path_segments) <= \
                    self.index_of_1st_local_path_segment:
                    index_of_last_virtual_path_segment_plus_1 = \
                        len(logical_path_segments) - 1
                else:
                    index_of_last_virtual_path_segment_plus_1 = \
                        self.index_of_1st_local_path_segment
                #print("process 1 thru " + \
                #    str(index_of_last_virtual_path_segment_plus_1 - 1))
                
                result_list = \
                    self.db_operator.mapLogicalPathsToDisplayNames(
                        logical_path_segments\
                            [1:index_of_last_virtual_path_segment_plus_1])
                        
                for index in \
                    range(self.index_of_1st_local_path_segment, 
                        len(logical_path_segments) - 1, 1):
                    
                    result_list.append(
                        {"logical": str(logical_path_segments[index]) + '/', 
                        "display": str(logical_path_segments[index])})
            
            return_value[one_logical_path] = result_list
        
        # end of for
    
        return return_value
    
    # end of function _prepareToDisplayLogicalPath
    
    ##
    # @brief Find the interface pair which best satisfies the requirements
    #        on src and dst partner systems.
    #
    # @param partner_system_src The source partner system.
    # @param partner_system_dst The destination partner system.
    # @param network_type_list  A priority list of network types.
    # @param IP_version_list    A priority list of IP versions.
    # @param bandwidth_range    A 2-tuple with min and max BW requirements.
    # 
    # @return A 2-tuple of one interface on each of source host and destination
    #         host respectively.
    def _findBestNetworkInterfacePair(
        partner_system_src,
        partner_system_dst,
        network_type_list,
        IP_version_list,
        bandwidth_range
        ):
    
        pass
    
    # end of function _findBestNetworkInterfacePair
    
    def _copyPath(self, **call):
    
        try:
            # check no. of arguments
            if not len(call["arguments"]["paths"]) == 2:
                raise parse_exception_wrong_no_paths(
                        "incorrect number of paths")
                
            # check permissions
            # extract parent path
            path_segments = call["arguments"]["paths"][0].split('/')
            if call["arguments"]["paths"][0]\
                [len(call["arguments"]["paths"][0]) - 1] == '/':
                parent_path = '/'.join(path_segments[:len(path_segments) - 1])
            else:
                parent_path = '/'.join(path_segments[:len(path_segments)])
            #print("parent: " + str(parent_path))
            
            if not self._hasPermissionsForPaths(\
                        call["effective_user"], \
                        {call["arguments"]["paths"][0]: "r",
                        parent_path: "w"}):
                raise parse_exception_insufficient_permissions(\
                        "insufficient permissions")
                        
            #print("logical path list: " + str(call["arguments"]["paths"]))
                    
            if self._belongToSameNonBICLSUiRODSPrivateUser(
                call["arguments"]["paths"]):
                # start cp on one partner system
                
                #print("local copy")
                
                # logical to physical mapping      
                logical_to_physical_information = \
                    self._prepareToManipulatePartnerSystems(\
                        call["arguments"]["paths"])
            
                # launch logical operation
                # logical paths are provided in pairs
                partner_system = \
                    logical_to_physical_information\
                        [call["arguments"]["paths"][1]]
                local_path_list = \
                    partner_system.getLocalPaths()
                partner_system = \
                    logical_to_physical_information\
                        [call["arguments"]["paths"][0]]
                partner_system.addLocalPaths(local_path_list)
                
                partner_system.prepareDataTransfer("local")
                
                result = partner_system.launchFileOperation("copy")
                return_value = result
                
            elif self._areOnUNIXStoragesOrBICLSUiRODS(
                call["arguments"]["paths"]):
                ##### LOGIC #####
                # if a path in BIC-LSU iRODS
                    # set physical path in partner system object
                # decide communication direction
                # if support bbcp
                    # pass direction as a parameter
                    # start bbcp from trans control partner system
                # else if support fdt
                    # if dst can be a server
                        # start fdt server at dst partner system
                        # start fdt client at src partner system
                        # client push
                    # else
                        # start fdt client at dst partner system
                        # start fdt server at src partner system
                        # client pull
                # if dst path in BIC-LSU iRODS
                    # register copied file to iRODS
                ##########
                    
                print("remote p2p copy")
                
                # retrieve task-aware network information
                TAN_information_directory = \
                {"validation": "passphrase",
                "scheduler interface": "shed.biclsu.lsu.edu",
                "scheduler interface IP version": "IPv4",
                "scheduler port": "12321",
                "reporter": "src"}
                TAN_information_directory = None
                    
                # logical to physical mapping      
                logical_to_physical_information = \
                    self._prepareToManipulatePartnerSystems(\
                        call["arguments"]["paths"]) 
                #print("log to phy: " + str(logical_to_physical_information))
                    
                iRODS_destination = False
                    
                for logical_path, partner_system in \
                    logical_to_physical_information.iteritems():
                    if partner_system.getResourceServerType() == \
                        "iRODS":
                        # is an BIC-LSU iRODS path
                        # change server type to GNU Utilities
                        #partner_system["host information"]["server type"] = \
                        #    "GNU Utilities"
                        #partner_system["host information"]["server version"] = \
                        #    "8"
                        # change local path
                        # The value varies with 
                        # iRODS Resource server configuration.
                        BICLSUiRODS_physical_path = \
                            "/SSDRAID0/irods/home/" + \
                            partner_system.getPrivateUser() + '/' + \
                            partner_system.getRelativePaths()[0]
                        # insert the physical path as the 1st local path
                        # the iRODS path becomes the 2nd
                        partner_system.addLocalPaths(
                            [BICLSUiRODS_physical_path], index=0)
                        # copy as BICLSU iRODS admin.
                        # transfer applications reside in BICLSU iRODS admin's 
                        # storage on the physical Linux host.
                        admin_information = \
                            self.getBICLSUiRODSAdminInformation(
                                )
                        # get supported transfer applications
                        transfer_application_list = \
                            self.db_operator.getTransferApplicationOf()
                        partner_system.setPrivateUser("irods")
                        partner_system.setTransferApplication(
                            transfer_application_list)
                        if logical_path == \
                            call["arguments"]["paths"][1]:
                            # destination path
                            iRODS_destination = True
                        print("irods par sys: " + str(partner_system.partner_system_information))
                
                logical_path_source = call["arguments"]["paths"][0]
                logical_path_destination = call["arguments"]["paths"][1]
                
                partner_system_source = \
                    logical_to_physical_information\
                        [logical_path_source]
                partner_system_destination = \
                    logical_to_physical_information\
                        [logical_path_destination]
                        
                transfer_applications_source = \
                    partner_system_source.getTransferApplication()
                transfer_applications_destination = \
                    partner_system_destination.getTransferApplication()
                    
                #print("trans src: " + str(transfer_applications_source))
                #print("trans dst: " + str(transfer_applications_destination))
                
                # find the most preferred transfer application
                found = False
                for index_src_system in \
                    range(0, len(transfer_applications_source), 1):
                                    
                    for index_dst_system in \
                        range(0, len(transfer_applications_destination), 1):
                                        
                        transfer_application_source = \
                            transfer_applications_source\
                                [index_src_system]
                                
                        if transfer_application_source["name"] == \
                            transfer_applications_destination\
                                [index_dst_system]["name"]:
                            transfer_application_destination = \
                                transfer_applications_destination\
                                    [index_dst_system]
                            found = True
                            break
                            
                    if found:
                        break
                        
                return_value = {}
                        
                if not found:
                    return_value["status"] = False
                    return_value["message"] = "Cannot find a common trans app"
                    return return_value
                 
                #print("common trans: " + \
                #    str(transfer_application_source["name"]))
                    
                # determine the communication order
                if transfer_application_destination["server"]:
                    # source as client
                    # client push
                    direction = True
                elif transfer_application_source["server"]:
                    # source as server
                    # client pull
                    direction = False
                else:
                    return_value["status"] = False
                    return_value["message"] = \
                        "Both ends are protected by firewalls."
                    return return_value
                    
                if transfer_application_source["name"] in \
                    ["BBCP"]:
                    
                    # logical to physical mapping    
                    # TO DO: directly launch BBCP from Command Service server
                    logical_path = \
                        self._get3rdPartyTransmissionControlResourcePath()
                    logical_to_physical_information = \
                        self._prepareToManipulatePartnerSystems(
                            [logical_path])
                    
                    # start 3rd-party transfer
                    partner_system_control = \
                        logical_to_physical_information[logical_path]
                        
                    transfer_application_list = \
                        partner_system_control.getTransferApplication()
                        
                    for one_application in transfer_application_list:
                        if one_application["name"] == \
                            transfer_application_source["name"]:
                            transfer_application_3rd_party = one_application
                        
                    # TO DO: make reservation optional
                    # reserve data ports
                    if direction:
                        # destination is server
                        private_resource_server = \
                            partner_system_destination.getResourceId()
                        data_ports = \
                            self.db_operator.reserveDataPorts(
                                private_resource_server, 
                                transfer_application_destination\
                                    ["write parallelism"])
                    else:
                        private_resource_server = \
                            partner_system_source.getResourceId()
                        data_ports = \
                            self.db_operator.reserveDataPorts(
                                private_resource_server, 
                                transfer_application_source\
                                    ["write parallelism"])
                        
                    partner_system_control.prepareDataTransfer(
                        "3rd-party",
                        transfer_application = \
                            [transfer_application_source,
                            transfer_application_destination,
                            transfer_application_3rd_party],
                        transfer_direction = \
                            direction,
                        source_information = \
                            partner_system_source,
                        destination_information = \
                            partner_system_destination,
                        server_ports = data_ports,
                        TAN_information = TAN_information_directory)

                    result = partner_system_control.launchFileOperation("copy")
                    
                    # release data ports
                    self.db_operator.releaseDataPorts(
                        private_resource_server,
                        data_ports)
                
                    return_value = result
                
                elif transfer_application_source["name"] in \
                    ["FDT"]:
                
                    if direction:
                        server = partner_system_destination
                        client = partner_system_source
                        transfer_application_server = \
                            transfer_application_destination
                        transfer_application_client = \
                            transfer_application_source
                    else:
                        client = partner_system_destination
                        server = partner_system_source
                        transfer_application_server = \
                            transfer_application_source
                        transfer_application_client = \
                            transfer_application_destination
                        
                    # reserve server listening port
                    private_resource_server = \
                        server.getResourceId()
                    control_ports = \
                        self.db_operator.reserveDataPorts(
                            private_resource_server, 1)
                        
                    server.prepareDataTransfer(
                        "client-server",
                        transfer_application = \
                            transfer_application_server,
                        transfer_direction = \
                            direction,
                        role = "server",
                        source_information = \
                            partner_system_source,
                        destination_information = \
                            partner_system_destination,
                        server_ports = control_ports,
                        TAN_information = TAN_information_directory)
                        
                    client.prepareDataTransfer(
                        "client-server",
                        transfer_application = \
                            transfer_application_client,
                        transfer_direction = \
                            direction,
                        role = "client",
                        source_information = \
                            partner_system_source,
                        destination_information = \
                            partner_system_destination,
                        server_ports = control_ports,
                        TAN_information = TAN_information_directory)
                
                    # launch logical operation
                    # start server
                    result = server.launchFileOperation("copy")
                    
                    if not result["exit status"] == 0:
                        return_value["status"] = False
                        return_value["message"] = \
                            "server did not properly start."
                        return json.dumps(return_value)
                    # start client
                    result = client.launchFileOperation("copy")
                    #print("result " + str(result))
                    
                    if not result["exit status"] == 0:
                        return_value["status"] = False
                        return_value["message"] = \
                            "client did not properly finish."
                    
                    # release server listening port
                    self.db_operator.releaseDataPorts(
                        private_resource_server,
                        control_ports)
                    
                # if destination is a BIC-LSU iRODS Resource, register
                if iRODS_destination:
                    # re-use the destination partner system
                    print("loc path: " + str(partner_system_destination.getLocalPaths()))
                    result = partner_system_destination.\
                                launchFileOperation("iRODS_register")
                    print("result: " + str(result))
                
            else:
                # regular iRODS, HDFS, OpenStack Swift
                print("upload/download w/ client")
                return_value["status"] = False
                return_value["message"] = \
                    "Operation not implemented yet in this version."
            
        except Exception as e:
            print("Exception: " + e.message)
            return_value["status"] = False
            return_value["message"] = str(e.message)
            # log failure
            #self.log_file_prefix + time + user + command + process
            
        # end of try
        
        return json.dumps(return_value)
    
    # end of function _copyPath
    
    def _belongToSameNonBICLSUiRODSPrivateUser(self, logical_paths):
    
        private_resources = []
        
        for one_path in logical_paths:
        
            path_segments = one_path.split('/')
            private_resources.append(path_segments[3])
        
        user_list = \
            self.db_operator.getPrivateUsers(private_resources)
        #print("users: " + str(user_list))
        
        if user_list:
        
            return_value = True
            for one_user in user_list:
                if one_user["server type"] == 'iRODS':
                    return_value = False
                    break
                    
            if return_value and not len(user_list) == 1:
                return_value = False
                
        else:
            return_value = False
        
        print("same non BICLSU iRODS user: " + str(return_value))
        
        #import sys
        #sys.exit()
        
        return return_value
    
    # end of function _belongToSameNonBICLSUiRODSPrivateUser
    
    def _areOnUNIXStoragesOrBICLSUiRODS(self, logical_paths):
    
        private_resources = []
        
        for one_path in logical_paths:
        
            path_segments = one_path.split('/')
            private_resources.append(path_segments[3])
    
        storage_service_list = \
            self.db_operator.getStorageServicesOf(private_resources)
        #print("storage srv: " + str(storage_service_list))
            
        if storage_service_list:
            UNIX_storage_services = \
                ["GNU_Utilities", "BSD_Utilities", "iRODS"]
            if set(storage_service_list).\
                        issubset(set(UNIX_storage_services)):
                return True
            else:
                return False
        else:
            return False
    
    # end of function _areOnUNIXStoragesOrBICLSUiRODS
    
    def _get3rdPartyTransmissionControlResourcePath(self):
    
        result_list = \
            self.db_operator.get3rdPartyTransmissionControlInformation()
        #print("trans ctl: " + str(result_list))    
        
        logical_path = "/0/" + \
            str(result_list[0][0]) + '/' + \
            str(result_list[0][1]) + '/'
        #print("logical path: " + str(logical_path))
        
        return logical_path
    
    # end of function _get3rdPartyTransmissionControlResource
    
    def _getBICLSUiRODSAdminResourcePath(self, path_to_be_managed):
    
        path_to_be_managed_segments = path_to_be_managed.split('/')
    
        result_list = \
            self.db_operator.getBICLSUiRODSAdminInformation(
                path_to_be_managed_segments[3])
        print("biclsu irods admin: " + str(result_list))    
        
        logical_path = "/0/" + \
            str(result_list[0][0]) + '/' + \
            str(result_list[0][1]) + '/'
        #print("logical path: " + str(logical_path))
    
        return logical_path
    
    # end of function _getBICLSUiRODSAdminResourcePath
    
    def _getBICLSUiRODSFullControlResourcePath(self, path_to_be_managed):
    
        path_to_be_managed_segments = path_to_be_managed.split('/')
    
        result_list = \
            self.db_operator.getBICLSUiRODSFullControlInformation(
                path_to_be_managed_segments[3])
        print("biclsu irods full ctl: " + str(result_list))    
        
        # TO DO
        # support auto region
        logical_path = "/0/" + \
            str(result_list[0][0]) + '/' + \
            str(result_list[0][1]) + '/'
        #print("logical path: " + str(logical_path))
    
        return logical_path
    
    # end of function _getBICLSUiRODSFullControlResourcePath
    
    def _generateUNIXUserNameString(self, length, is_start):
    
        character_set = \
            ("ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz"
            "0123456789"
            "_")
        
        if is_start:
            return_string = \
                character_set[random.randint(0, len(character_set) - 12)]
                #exclude the numbers and under score character
            length = length - 1
        else:
            return_string = ""
        random_bytes = os.urandom(length)
        set_length = len(character_set)
        index = []
        for one_byte in random_bytes:
            index.append(int(set_length * (ord(one_byte) / 256.0)))
        for one_index in index:
            return_string = return_string + character_set[one_index]
        
        return return_string
    
    # end of function _generateUNIXUserNameString
    
    def _generatePasswordString(self, length):
    
        character_set = \
            ("ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz"
            "0123456789"
            "_!@#$%^&*()")
    
        return_string = ""
        random_bytes = os.urandom(length)
        set_length = len(character_set)
        index = []
        for one_byte in random_bytes:
            index.append(int(set_length * (ord(one_byte) / 256.0)))
        for one_index in index:
            return_string = return_string + character_set[one_index]
        
        return return_string
    
    # end of function _generatePasswordString
    
    def _addUser(self, **call):
        
        try:
            
            # check parameters
            if not "user_info" in call["arguments"] or \
                not len(call["arguments"]["user_info"]) > 0:
                raise parse_exception_wrong_no_arguments(
                        "incorrect number of arguments")
            
            #TO DO:
            #check privilege
            
            for one_info in call["arguments"]["user_info"]:
                
                # a user can add only himself/herself
                one_info["SSO_user"] = call["effective_user"]
                # create BIC-LSU iRODS user name and password
                BICLSU_iRODS_user = \
                    self._generateUNIXUserNameString(8, True) + \
                    call["effective_user"] + \
                    self._generateUNIXUserNameString(8, False)
                one_info["iRODS_user"] = BICLSU_iRODS_user
                BICLSU_iRODS_password = \
                    self._generatePasswordString(64)
                one_info["iRODS_password"] = BICLSU_iRODS_password
                #print("usr:" + BICLSU_iRODS_user + \
                #       ", pwd:" + BICLSU_iRODS_password)
                
                #TO DO:
                #add BIC-LSU iRODS user
                #self._createUserOnSharedStorage()
                
            # end of for
                        
            self.db_operator.addUser(call["arguments"]["user_info"])
                
            status = True
            result = None
            
        except Exception as e:
            print("Exception: " + str(e))
            status = False
            result = {}
            # log failure
            #self.log_file_prefix + time + user + command + process
            
        # end of try
    
        return_value = {\
            "status": status,
            "result": result
        }
    
        return json.dumps(return_value)
        
    # end of function _addUser
    
    def _listUser(self, **call):
    
        pass
    
    # end of _listUser
    
    def _addGroup(self, **call):
    
        try:
            
            # check parameters
            #call["arguments"]["group_info"]
            #[{"display_name": <name>, 
            #"front_end_group": <group Id>,
            #"front_end_creator": <user Id>},...]
            if not "group_info" in call["arguments"]:
                raise parse_exception_wrong_no_arguments(
                        "incorrect number of arguments")
            
            #TO DO
            #check privilege
                       
            for one_info in call["arguments"]["group_info"]:
                #generate BIC-LSU iRODS group name
                BICLSU_iRODS_group = \
                    "BICLSU_iRODS_group_" + str(one_info["front_end_group"])
                #print("grp: " + str(BICLSU_iRODS_group))
                one_info["iRODS_group"] = BICLSU_iRODS_group
            
            #TO DO
            #add BIC-LSU iRODS group
            
            result = \
                self.db_operator.addGroup(call["arguments"]["group_info"])
            
            for one_result in result:
                # augment info for adding members
                one_result["front_end_user"] = one_result["front_end_creator"]
                # the creator must be the administrator while creating
                one_result["is_admin"] = 1
                
            self.db_operator.addMember(result)
            
            for one_result in result:
                # clean the augmented info for adding members
                one_result.pop("front_end_user", None)
                one_result.pop("is_admin", None)
                        
            status = True
                        
        except Exception as e:
            traceback.print_exc()
            print("Exception, " + str(e) + ": " + str(e.message))
            status = False
            result = {}
            # log failure
            #self.log_file_prefix + time + user + command + process
            
        # end of try
    
        return_value = {\
            "status": status,
            "result": result
        }
    
        #print("ret val: " + str(return_value))
        return json.dumps(return_value)
    
    # end of function _addGroup
    
    def _listGroup(self, **call):
    
        try:
            # check arguments
            #none
            #if not "user" in call["arguments"] or \
            #    not len(call["arguments"]["user"]) > 0:
            #    raise parse_exception_wrong_no_arguments(
            #            "incorrect number of arguments")
            
            # check privilege
            #none
        
            result = \
                self.db_operator.listGroup(
                    [call["effective_user"]])
                
            status = True
        
        except Exception as e:
            print("Exception: " + e.message)
            status = False
            result = {}
            # log failure
            #self.log_file_prefix + time + user + command + process
            
        # end of try
    
        return_value = {\
            "status": status,
            "result": result
        }
        
        #print(str(return_value))
        return json.dumps(return_value)
    
    # end of function _listGroup
    
    def _editGroup(self, **call):
        
        try:
            if not "group_info" in call["arguments"] or \
                not len(call["arguments"]["group_info"]) > 0:
                raise parse_exception_wrong_no_arguments(
                        "incorrect number of arguments")
            
            # check privilege
            #none
        
            result = \
                self.db_operator.editGroup(
                    call["arguments"]["group_info"])
                
            status = True
        
        except Exception as e:
            print("Exception: " + e.message)
            status = False
            result = {}
            # log failure
            #self.log_file_prefix + time + user + command + process
            
        # end of try
    
        return_value = {\
            "status": status,
            "result": result
        }
        
        #print(str(return_value))
        return json.dumps(return_value)
    
    # end of function _editGroup
    
    def _removeGroup(self, **call):
    
        try:
            if not "group_info" in call["arguments"] or \
                not len(call["arguments"]["group_info"]) > 0:
                raise parse_exception_wrong_no_arguments(
                        "incorrect number of arguments")
            
            # check privilege
            #none
        
            result = \
                self.db_operator.removeGroup(
                    call["arguments"]["group_info"])
                
            status = True
        
        except Exception as e:
            print("Exception: " + e.message)
            status = False
            result = {}
            # log failure
            #self.log_file_prefix + time + user + command + process
            
        # end of try
    
        return_value = {\
            "status": status,
            "result": result
        }
        
        #print(str(return_value))
        return json.dumps(return_value)
        
    # end of function _removeGroup
    
    def _listHostWithApplication(self, **call):
    
        try:
            # check no. of arguments
            if not "application" in call["arguments"] or \
                not len(call["arguments"]["application"]) > 0:
                raise parse_exception_wrong_no_arguments(
                        "incorrect number of arguments")
        
            # verify the values of arguments
            #for one_service in call["arguments"]["service"]:
            #    if one_service not in \
            #        ["storage", "scheduling"]:
            #        raise parse_exception_not_supported(
            #            "service not supported")
            
            result = \
                self.db_operator.listHostWithApplication(
                    call["arguments"]["application"])
                    
            status = True
                
        except Exception as e:
            print("Exception: " + str(e))
            status = False
            result = {}
            # log failure
            #self.log_file_prefix + time + user + command + process
            
        # end of try
    
        return_value = {\
            "status": status,
            "result": result
        }
    
        return json.dumps(return_value)
    
    # end of function _listHost
    
    ##
    # @breif    Register a private resource.  Renew the control credential
    #           if the resource has been registered.
    #
    def _addResource(self, **call):
    
        try:
            #check no. of arguments
            # TO DO
            # check each resource list element
            if "resource" not in call["arguments"]:
                raise parse_exception_wrong_no_of_arguments(
                    "no resource to add")
                    
            # TO DO
            # check privilege
            
            # {"resource":[
            #{"host": <ID>, "service": <ID>, "display_name": <display name>,
            #"private_user": <pvt user>,
            #"shell": <sh>, "root": <root path>, "allow_ACL": 0/1}, ...]}
            # TO DO
            #make shell optional
            for one_resource in call["arguments"]["resource"]:
                user_Ids = \
                    self.db_operator.getUserIdOf([call["effective_user"]])
            
                one_resource["biclsu_user"] = \
                    user_Ids[unicode(call["effective_user"])]
                
                # TO DO
                #generate credential according to resource type
                #generate an SSH Key Pair
                temp_file_tuple = tempfile.mkstemp(
                    prefix = "id_PKI_",
                    dir = self.SSH_key_pair_info["prefix path"])
                #print("pki path: " + temp_file_tuple[1])
                credential_path_segments = temp_file_tuple[1].split('/')
                credential_path = \
                    credential_path_segments[len(credential_path_segments) - 1]
                result = \
                    SSH_KeyPair.SSH_KeyPair().generateControlCredential(
                        "rsa", 3072,
                        self.SSH_key_pair_info["prefix path"], credential_path)
                if not result[0]:
                    raise Exception("Error while generating SSH Key Pair")
                
                #add control credential
                one_resource["credential_category"] = "OpenSSH_PKI" 
                one_resource["credential_path"] = credential_path
                one_resource["public_key"] = result[1]
            
            result = \
                self.db_operator.addResource(call["arguments"]["resource"])
                
            for key,value in result.iteritems():
                if "old_credential_path" in value:
                    old_credential_path = \
                        value.pop("old_credential_path", None)
                    #os.remove(old_credential_path)
                    print("remove " + str(old_credential_path))
                    
            status = True
                
        except Exception as e:
            print("Exception: " + str(e))
            traceback.print_exc()
            status = False
            result = {}
            # log failure
            #self.log_file_prefix + time + user + command + process
            
        # end of try
    
        return_value = {\
            "status": status,
            "result": result
        }
    
        return json.dumps(return_value)
    
    # end of function _addResource
    
    def _listResource(self, **call):
    
        try:
            # check no. of arguments
            if not "category" in call["arguments"] or \
                not len(call["arguments"]["category"]) > 0:
                raise parse_exception_wrong_no_arguments(
                        "incorrect number of arguments")
        
            # verify the values of arguments
            for one_category in call["arguments"]["category"]:
                if one_category not in \
                    ["storage", "scheduling", \
                    "shell", "session","authentication"]:
                    raise parse_exception_not_supported(
                        "service not supported")
            
            result = \
                self.db_operator.listResource(
                    [{"user": call["effective_user"], 
                    "category": call["arguments"]["category"]}])
                    
            # TO DO
            # support autonamous region
            for one_result in result[call["effective_user"]]:
                one_result["path"] = \
                    "/0/" + \
                    str(one_result["user"]) + "/" + \
                    str(one_result["resource"]) + "/"
                    
            status = True
                
        except Exception as e:
            traceback.print_exc()
            print("Exception: " + str(e))
            status = False
            result = {}
            # log failure
            #self.log_file_prefix + time + user + command + process
            
        # end of try
    
        return_value = {\
            "status": status,
            "result": result
        }
    
        return json.dumps(return_value)
    
    # end of function _listResource
    
    def _listServiceOnHostWithCategory(self, **call):
    
        try:
            # check no. of arguments
            if not "category" in call["arguments"] or \
                not len(call["arguments"]["category"]) > 0 or \
                not "host" in call["arguments"]:
                raise parse_exception_wrong_no_arguments(
                        "incorrect number of arguments")
        
            # verify the values of arguments
            for one_category in call["arguments"]["category"]:
                if one_category not in \
                    ["storage","scheduling","shell","session","authentication"]:
                    raise parse_exception_not_supported(
                        "service not supported")
            
            result = \
                self.db_operator.listServiceOnHostWithCategory(
                    call["arguments"]["host"], call["arguments"]["category"])
                    
            status = True
                
        except Exception as e:
            print("Exception: " + str(e))
            status = False
            result = {}
            # log failure
            #self.log_file_prefix + time + user + command + process
            
        # end of try
    
        return_value = {\
            "status": status,
            "result": result
        }
    
        return json.dumps(return_value)
    
    # end of function _listServiceWithCategory
    
    def _getFriendship(self, **call):
    
        try:
            if len(call["arguments"]["users"]) < 1:
            
                raise parse_exception_wrong_no_arguments(
                        "incorrect number of arguments")
            
            # end of if
        
            result = \
                    self.db_operator.getFriendshipOf(
                        call["arguments"]["users"])
                
            status = True
        
        except Exception as e:
            print("Exception: " + str(e))
            status = False
            result = {}
            # log failure
            #self.log_file_prefix + time + user + command + process
            
        # end of try
    
        return_value = {\
            "status": status,
            "result": result
        }
        
        #print(str(return_value))
        return json.dumps(return_value)
    
    # end of function _getFriendshipOf
    
    def _addMember(self, **call):
    
        try:
            #check no. of arguments
            #TO DO
            #check each member list element
            if not "membership" in call["arguments"]:      
                raise parse_exception_wrong_no_arguments(
                    "no resource to add")
                    
            # TO DO
            # check privilege
            
            result = \
                self.db_operator.addMember(call["arguments"]["membership"])
                    
            status = True
                
        except Exception as e:
            print("Exception: " + str(e))
            status = False
            result = {}
            # log failure
            #self.log_file_prefix + time + user + command + process
            
        # end of try
    
        return_value = {\
            "status": status,
            "result": result
        }
    
        return json.dumps(return_value)
    
    # end of function _addMember
    
    def _listMember(self, **call):
    
        try:
            if not "group" in call["arguments"] or \
                not len(call["arguments"]["group"]) > 0:
                raise parse_exception_wrong_no_arguments(
                        "incorrect number of arguments")
            
            # check privilege
            #none
        
            result = \
                self.db_operator.listMember(
                    call["arguments"]["group"])
                
            status = True
        
        except Exception as e:
            print("Exception: " + e.message)
            status = False
            result = {}
            # log failure
            #self.log_file_prefix + time + user + command + process
            
        # end of try
    
        return_value = {\
            "status": status,
            "result": result
        }
        
        #print(str(return_value))
        return json.dumps(return_value)
    
    # end of function _listMember
    
    def _removeMember(self, **call):
        
        try:
            if not "membership" in call["arguments"] or \
                not len(call["arguments"]["membership"]) > 0:
                raise parse_exception_wrong_no_arguments(
                        "incorrect number of arguments")
            
            # TO DO
            # check privilege
        
            result = \
                self.db_operator.removeMember(
                    call["arguments"]["membership"])
                
            status = True
        
        except Exception as e:
            print("Exception: " + e.message)
            status = False
            result = {}
            # log failure
            #self.log_file_prefix + time + user + command + process
            
        # end of try
    
        return_value = {\
            "status": status,
            "result": result
        }
        
        #print(str(return_value))
        return json.dumps(return_value)
        
    # end of function _removeMembership
    
    def _listHostServiceWithCategoryApplication(self, **call):
           
        try:
            if not "condition" in call["arguments"] or \
                not len(call["arguments"]["condition"]) > 0:
                raise parse_exception_wrong_no_arguments(
                        "incorrect number of arguments")
            
            # TO DO
            # check privilege
        
            result = \
                self.db_operator.listHostServiceWithCategoryApplication(
                    call["arguments"]["condition"])
                
            status = True
        
        except Exception as e:
            traceback.print_exc()
            print("Exception: " + e.message)
            status = False
            result = {}
            # log failure
            #self.log_file_prefix + time + user + command + process
            
        # end of try
    
        return_value = {\
            "status": status,
            "result": result
        }
        
        #print(str(return_value))
        return json.dumps(return_value)
    
    # end of function _listHostServiceWithCategoryApplication
    
    def _addApplication(self, **call):
    
        try:
            #check no. of arguments
            if not "application_info" in call["arguments"]:
                raise parse_exception_wrong_no_arguments(
                    "incorrect number of arguements.")
                
            # TO DO
            #check privilege
        
            web_portal = \
                self._prepareToManipulatePartnerSystems(
                    [self.job_script_parsing["uploaded script resc path"]])[
                    self.job_script_parsing["uploaded script resc path"]]
                    
            #parameter = {}
            #parameter["host"] = 
            #parameter["service"] = call["arguments"]["service"]
            #parameter["script_portal_path"] = \
            #    call["arguments"]["script_portal_path"]
            
            # create a unique file on cmd svc
            temp_file_tuple = tempfile.mkstemp(
                prefix = "tagged_",
                dir = self.job_script_parsing["valid tagged script repo path"])
            tagged_script_cmd_svc_path = temp_file_tuple[1]
            print("tagged script cmd svc path: " + tagged_script_cmd_svc_path)
            #parameter["script_cmd_svc_path"] = tagged_script_cmd_svc_path            
            #web_portal.addOperationSpecificParameters(parameter)
            
            #transfer the tagged script from portal to cmd svc
            #print("uploaded script path: " + \
            #    web_portal.partner_system_information["local paths"][0] + \
            #    call["arguments"]["application_info"][0]["script_portal_path"])
            web_portal.sendSmallFileToCommandService(
                web_portal.partner_system_information["local paths"][0] + \
                call["arguments"]["application_info"][0]["script_portal_path"], 
                tagged_script_cmd_svc_path)
            
            #convert the tagged job script into a GUI skeleton 
            #and a converted job script.
            output_json = \
                subprocess.check_output([
                    "python",
                    self.job_script_parsing["to skeleton parser path"], 
                    tagged_script_cmd_svc_path])
            output = json.loads(output_json)
            #print("skel: " + json.dumps(output["gui skeleton"]))
            
            # create a unique file for the converted job script.
            temp_file_tuple = tempfile.mkstemp(
                prefix = "converted_",
                dir = self.job_script_parsing["converted script repo path"])
            converted_script_cmd_svc_path = temp_file_tuple[1]
            print("converted script cmd svc path: " + \
                converted_script_cmd_svc_path)
            
            #write the converted job script
            with open(converted_script_cmd_svc_path, "w") as converted_script:
                converted_script.write(output["converted script"])
            print("script is stored on cmd svc")
            
            #insert application information into DB.
            if "application" in call["arguments"]["application_info"][0]:
                result = \
                    self.db_operator.addComputingFacilityForApplication([(
                        int(call["arguments"]["application_info"][0]\
                            ["application"]),
                        int(call["arguments"]["application_info"][0]\
                            ["host"]),
                        int(call["arguments"]["application_info"][0]\
                            ["service"]),
                        tagged_script_cmd_svc_path.split("/")[-1],
                        json.dumps(output["gui skeleton"]),
                        converted_script_cmd_svc_path.split("/")[-1])])
            elif "display_name" in call["arguments"]["application_info"][0]:
                result = \
                    self.db_operator.addNewApplication([(
                        call["arguments"]["application_info"][0]\
                            ["display_name"],
                        int(call["arguments"]["application_info"][0]\
                            ["host"]),
                        int(call["arguments"]["application_info"][0]\
                            ["service"]),
                        tagged_script_cmd_svc_path.split("/")[-1],
                        json.dumps(output["gui skeleton"]),
                        converted_script_cmd_svc_path.split("/")[-1])])
                
            status = True
        
        except subprocess.CalledProcessError as e:
            # remove invalid tagged script
            os.remove(tagged_script_cmd_svc_path)
            print("CalledProcessError: " + e.message)
            status = False
            result = {}
            # log failure
            #self.log_file_prefix + time + user + command + process
        
        except Exception as e:
            traceback.print_exc()
            print("Exception " + str(e) + ": " + str(e.message))
            status = False
            result = {}
            # log failure
            #self.log_file_prefix + time + user + command + process
            
        # end of try
    
        return_value = {\
            "status": status,
            "result": result
        }
        
        #print(str(return_value))
        return json.dumps(return_value)
    
    # end of _addApplication
    
    ##
    # @brief    List all available applications of a user.
    #
    # @return   A dictionary of
    def _listApplication(self, **call):
    
        try:
            #check no. of arguments
            if not "purpose" in call["arguments"]:
                raise parse_exception_wrong_no_arguments(
                    "incorrect number of arguements.")
            
            #check privilege
            #none
            if call["arguments"]["purpose"] == "add":
                result = \
                    self.db_operator.listApplicationForAdding()
            elif call["arguments"]["purpose"] == "launch" or \
                call["arguments"]["purpose"] == "get":
                result = \
                    self.db_operator.listApplicationForLaunching(
                        [call["effective_user"]])
                
            status = True
        
        except Exception as e:
            traceback.print_exc()
            print("Exception: " + e.message)
            status = False
            result = {}
            # log failure
            #self.log_file_prefix + time + user + command + process
            
        # end of try
    
        return_value = {\
            "status": status,
            "result": result
        }
        
        #print(str(return_value))
        return json.dumps(return_value)
    
    # end of function _listApplication
    
    def _getGUIOfApplication(self, **call):
    
        try:
            #check no. of arguments
            if not "application" in call["arguments"] or \
                not "host" in call["arguments"] or \
                not "service" in call["arguments"]:
                raise parse_exception_wrong_no_arguments(
                    "incorrect number of arguements.")
            
            #check privilege
            #none
            
            result = \
                self.db_operator.getGUIOfApplication([(
                    call["arguments"]["application"],
                    call["arguments"]["host"],
                    call["arguments"]["service"])])
                
            status = True
        
        except Exception as e:
            traceback.print_exc()
            print("Exception: " + e.message)
            status = False
            result = {}
            # log failure
            #self.log_file_prefix + time + user + command + process
            
        # end of try
    
        return_value = {\
            "status": status,
            "result": result
        }
        
        #print(str(return_value))
        return json.dumps(return_value)
        
    # end of function _getGUIForApplication 
    
    def _getApplication(self, **call):
    
        try:
            #check no. of arguments
            if not "application" in call["arguments"] or \
                not "host" in call["arguments"] or \
                not "service" in call["arguments"]:
                raise parse_exception_wrong_no_arguments(
                    "incorrect number of arguements.")
            
            #check privilege
            #none
            
            result = \
                self.db_operator.getApplication([(
                    call["arguments"]["application"],
                    call["arguments"]["host"],
                    call["arguments"]["service"])])
                
            #read tagged job script
            key = \
                str((int(call["arguments"]["application"]), 
                    int(call["arguments"]["host"]),
                    int(call["arguments"]["service"])))
            #print("key: " + str(key))
            tagged_script_path = \
                self.job_script_parsing["valid tagged script repo path"] + \
                result[key]["tagged"]
            #print("script p: " + str(tagged_script_path))
            with open(tagged_script_path, "r") as tagged_script:
                content = tagged_script.read()
            #wrap script content
            #print("script content: " + str(content))
            result = str(content)
                
            status = True
        
        except Exception as e:
            traceback.print_exc()
            print("Exception, " + str(e) + ": " + e.message)
            status = False
            result = {}
            # log failure
            #self.log_file_prefix + time + user + command + process
            
        # end of try
    
        return_value = {\
            "status": status,
            "result": result
        }
        
        #print(str(return_value))
        return json.dumps(return_value)
    
    # end of function _getApplication

    def _launchApplication(self, **call):
    
        try:
            # check no. of args
            if not "input" in call["arguments"] or \
                not "resource" in call["arguments"] or \
                not "host" in call["arguments"] or \
                not "service" in call["arguments"] or \
                not "application" in call["arguments"]:            
                raise parse_exception_wrong_no_arguments(
                        "incorrect number of arguments")
        
            #TO DO
            # check permission
            
            user_Ids = \
                self.db_operator.getUserIdOf([call["effective_user"]])
            current_user_Id = user_Ids[call["effective_user"]]            
            # TO DO
            # add autonomous region support
            #resource_path = "/0/" + str(current_user_Id) + "/" + \
            #        call["arguments"]["resource"] + "/"
            #print("resc path: " + str(resource_path))
            computing_facility = \
                self._prepareToManipulatePartnerSystems(
                    [call["arguments"]["resource"]])\
                    [call["arguments"]["resource"]]
            #print("par sys: " + str(computing_facility.\
            #    partner_system_information["host information"]))
                
            parameter = {}
            
            #collect user input parameters
            #TO DO:
            #verify call["input"]'s format
            parameter["GUI input"] = call["arguments"]["input"]
            #print("GUI in: " + parameter["GUI input"])
            logical_to_physical = {}
            for one_component in parameter["GUI input"]:
            
                if (one_component["is_path"]):
                    if not one_component["value"] in logical_to_physical:
                        logical_to_physical[one_component["value"]] = []
                    logical_to_physical[one_component["value"]].append(
                        one_component)
                #remove is_path attribute
                del one_component["is_path"]
                    
            results = \
                self._prepareToManipulatePartnerSystems(
                    [path for path in logical_to_physical.keys()])
            
            for key,value in results.iteritems():
                for one_component in logical_to_physical[key]: 
                    one_component["value"] = \
                        value.partner_system_information["local paths"][0]
            print("GUI in w/ phy p: " + str(parameter["GUI input"]))
            
            #locate converted job script
            resource_Id = int(call["arguments"]["resource"].split('/')[3])
            host_Id = self.db_operator.getHostUserIdOfResource(
                [resource_Id])[resource_Id]["host"]
            scheduling_service_Id = int(call["arguments"]["service"])
            application_Id = int(call["arguments"]["application"])
            print("h: " + str(host_Id) + \
                ", s: " + str(scheduling_service_Id) + \
                ", a: " + str(application_Id))
            
            parameter["converted script path"] = \
                self.job_script_parsing["converted script repo path"] + \
                self.db_operator.getApplication(
                    [(application_Id, host_Id, scheduling_service_Id)])\
                    [str((application_Id, host_Id, scheduling_service_Id))]\
                    ["converted"]
                    
            #parameter["scheduling service"] = \
            #    self.db_operator.getSchedulingServiceNameOf(
            #        scheduling_service_Id)
                    
            parameter["combining parser path"] = \
                self.job_script_parsing["to script parser path"]
                
            #pass operation specific parameters
            computing_facility.addOperationSpecificParameters(parameter)
            #print("partner sys op param: " + \
            #    str(computing_facility.operation_parameter))
                    
            result = \
                computing_facility.controlJob("schedule")
                
            status = True
        
        except Exception as e:
            traceback.print_exc()
            print("Exception, " + str(e) + ": " + str(e.message))
            status = False
            result = {}
            # log failure
            #self.log_file_prefix + time + user + command + process
            
        # end of try
    
        return_value = {\
            "status": status,
            "result": result
        }
        
        #print(str(return_value))
        return json.dumps(return_value)
    
    # end of function _launchApplication
    
    def _monitorApplication(self, **call):
    
        try:
            # check no. of args
            if not "resource" in call["arguments"]:
                raise parse_exception_wrong_no_arguments(
                        "incorrect number of arguments")
            
            # TO DO
            # check permission
            
            computing_facility = \
                self._prepareToManipulatePartnerSystems(
                    call["arguments"]["resource"])\
                    [call["arguments"]["resource"][0]]
                    
            parameter = {}
            parameter["user"] = \
                computing_facility.partner_system_information\
                ["host information"]["private user"]
            computing_facility.addOperationSpecificParameters(parameter)
            
            result = \
                computing_facility.controlJob("monitor")
                
            status = True
        
        except Exception as e:
            traceback.print_exc()
            print("Exception: " + e.message)
            status = False
            result = {}
            # log failure
            #self.log_file_prefix + time + user + command + process
            
        # end of try
                
        return_value = {\
            "status": status,
            "result": result
        }
                
        #print(str(return_value))
        return json.dumps(return_value)
    
    # end of function _monitorApplication
    
    def _abortApplication(self, **call):
    
        try:
            # check no. of args
            if not "resource" in call["arguments"] or \
                not "job" in call["arguments"]:
                raise parse_exception_wrong_no_arguments(
                        "incorrect number of arguments")
            
            # TO DO
            # check permission
            
            computing_facility = \
                self._prepareToManipulatePartnerSystems(
                    [call["arguments"]["resource"]])\
                    [call["arguments"]["resource"]]
                    
            #prepare parameter
            parameter = {}
            parameter["job Id"] = call["arguments"]["job"]
            computing_facility.addOperationSpecificParameters(parameter)            
            result = \
                computing_facility.controlJob("abort")
                
            status = True
        
        except Exception as e:
            print("Exception: " + e.message)
            status = False
            result = {}
            # log failure
            #self.log_file_prefix + time + user + command + process
            
        # end of try
                
        return_value = {\
            "status": status,
            "result": result
        }
                
        #print(str(return_value))
        return json.dumps(return_value)
    
    # end of function _abortApplication
    
    def _addAnnotation(self, **call):
    
        try:
            # check no. of args
            if not "annotation_info" in call["arguments"] or \
                not len(call["arguments"]["annotation_info"]) > 0:
                raise parse_exception_wrong_no_arguments(
                        "incorrect number of arguments")
            
            # TO DO
            # check permission
            
            #annotation_info
            #[[<logical path>, <antn>], ...]
            # TO DO
            #support autonamous region
            for one_info in call["arguments"]["annotation_info"]:
                path_segments = one_info[0].split("/")
                one_info.append(path_segments[3])
                one_info.append("/".join(path_segments[4:]))
                #print("logic p: " + str(one_info))
            
            result = self.db_operator.addAnnotation(
                call["arguments"]["annotation_info"])
            #result
            #{<logical path>: <antn Id>, ...}
                            
            status = True
        
        except Exception as e:
            print("Exception: " + e.message)
            status = False
            result = {}
            # log failure
            #self.log_file_prefix + time + user + command + process
            
        # end of try
                
        return_value = {\
            "status": status,
            "result": result
        }
                
        #print(str(return_value))
        return json.dumps(return_value)
    
    # end of function _addAnnotation
    
    def _listAnnotation(self, **call):
    
        try:
            # check no. of args
            if not "path" in call["arguments"] or \
                not len(call["arguments"]["path"]) > 0:
                raise parse_exception_wrong_no_arguments(
                        "incorrect number of arguments")
            
            # TO DO
            # check permission
              
            result = self.db_operator.listAnnotation(
                call["arguments"]["path"])
            #result
            #{<logical path>: [(<antn Id>, <antn>),...],...}
                            
            status = True
        
        except Exception as e:
            print("Exception: " + e.message)
            status = False
            result = {}
            # log failure
            #self.log_file_prefix + time + user + command + process
            
        # end of try
                
        return_value = {\
            "status": status,
            "result": result
        }
                
        #print(str(return_value))
        return json.dumps(return_value)
    
    # end of function _listAnnotation
    
    def _editAnnotation(self, **call):
    
        try:
            # check no. of args
            if not "annotation_info" in call["arguments"] or \
                not len(call["arguments"]["annotation_info"]) > 0:
                raise parse_exception_wrong_no_arguments(
                        "incorrect number of arguments")
            
            # TO DO
            # check permission
            
            #annotation_info
            #[(<antn Id>, <antn>), ...]
            result = self.db_operator.editAnnotation(
                call["arguments"]["annotation_info"])
            #result
            #{<antn Id>: <T or F>,...}
                            
            status = True
        
        except Exception as e:
            print("Exception: " + e.message)
            status = False
            result = {}
            # log failure
            #self.log_file_prefix + time + user + command + process
            
        # end of try
                
        return_value = {\
            "status": status,
            "result": result
        }
                
        #print(str(return_value))
        return json.dumps(return_value)
    
    # end of function _editAnnotation
    
    def _removeAnnotation(self, **call):
    
        try:
            # check no. of args
            if not "annotation_info" in call["arguments"] or \
                not len(call["arguments"]["annotation_info"]) > 0:
                raise parse_exception_wrong_no_arguments(
                        "incorrect number of arguments")
            
            # TO DO
            # check permission
            
            #annotation_info
            #[<antn Id>,...]
            result = self.db_operator.removeAnnotation(
                call["arguments"]["annotation_info"])
            #result
            #{<antn Id>: <T or F>,...}
                            
            status = True
        
        except Exception as e:
            print("Exception: " + e.message)
            status = False
            result = {}
            # log failure
            #self.log_file_prefix + time + user + command + process
            
        # end of try
                
        return_value = {\
            "status": status,
            "result": result
        }
                
        #print(str(return_value))
        return json.dumps(return_value)
    
    # end of function _removeAnnotation
    
    # ONLY the owner user can manipulate ACLs
    
    ##
    # @breif    Add an ACL entry for a path/annotation.
    #
    def _addACL(self, **call):
    
        try:
            # check no. of args
            if not "ACL_info" in call["arguments"] or \
                not len(call["arguments"]["ACL_info"]) > 0:
                raise parse_exception_wrong_no_arguments(
                        "incorrect number of arguments")
            
            # TO DO
            # check permission
            
            #ACL_info
            #[{"ACL_type": "dir", "path": <logical path>, 
            #"IO_privilege": <priv>, "computing_privilege": <priv>,
            #"target_type": "group/user", "target_Id": <Id>, 
            #\"recursive\":"0/1"},
            #{"ACL_type": "antn", "Id": <antn Id>, 
            #"privilege": <priv>,
            #"target_type": "group/user", "target_Id": <Id>}, ...]
            
            #if dir ACL, separate resource path and relative path
            for one_info in call["arguments"]["ACL_info"]:
                if one_info["ACL_type"] == "directory":
                    path_segments = one_info["path"].split("/")
                    one_info["resource"] = path_segments[3]
                    one_info["relative path"] = "/".join(path_segments[4:])
                    #print("logic p: " + str(one_info))
                
            result = self.db_operator.addACL(call["arguments"]["ACL_info"])
            #result
            #{("dir",<path>,<IO privilege>,<computing privilege>,
            #<target type>,<target Id>,<recursive>): <dir ACL Id>,
            #("antn",<Id>,<privilege>,<target type>,<target Id>): <antn ACL Id>, 
            #...}
                
            status = True
        
        except Exception as e:
            traceback.print_exc()
            print("Exception: " + str(e.message))
            status = False
            result = {}
            # log failure
            #self.log_file_prefix + time + user + command + process
            
        # end of try
                
        return_value = {\
            "status": status,
            "result": result
        }
                
        #print(str(return_value))
        return json.dumps(return_value)
    
    # end of function _addACL
        
    ##
    # @brief    List all ACL entries of a path/annotation.
    #
    def _listACL(self, **call):
    
        try:
            # check no. of args
            if not "ACL_info" in call["arguments"] or \
                not len(call["arguments"]["ACL_info"]) > 0:
                raise parse_exception_wrong_no_arguments(
                        "incorrect number of arguments")
            
            # TO DO
            # check permission
            
            #ACL_info
            #[{"ACL_type": "dir", "path": <logical path>},
            #{"ACL_type": "antn", "Id": <antn Id>}, ...]
            
            #if dir ACL, separate resource path and relative path
            for one_info in call["arguments"]["ACL_info"]:
                if one_info["ACL_type"] == "directory":
                    path_segments = one_info["path"].split("/")
                    one_info["resource"] = path_segments[3]
                    one_info["relative path"] = "/".join(path_segments[4:])
                    #print("logic p: " + str(one_info))
                    
            result = self.db_operator.listACL(call["arguments"]["ACL_info"])
            #result
            #{("directory", <logical path>): 
            #[{"ACL_type": "dir", "Id": <dir ACL Id>, 
            #"target_type": <target type>, "target_Id": <target Id>,
            #"display_name": <target display name>, 
            #"IO_privilege": <priv>, "computing_privilege": <priv>}], 
            #("antn", <antn Id>):
            #[{"ACL_type": "antn", "Id": <antn ACL Id>, 
            #"target_type": <target type>, "target_Id": <target Id>,
            #"display_name": <target display name>, 
            #"privilege": <priv>}],
            #...}
                
            status = True
        
        except Exception as e:
            traceback.print_exc()
            print("Exception: " + str(e.message))
            status = False
            result = {}
            # log failure
            #self.log_file_prefix + time + user + command + process
            
        # end of try
                
        return_value = {\
            "status": status,
            "result": result
        }
                
        #print(str(return_value))
        return json.dumps(return_value)
    
    # end of function _listACL
    
    ##
    # @brief    Edit an ACL entry for a path/annotation.
    #
    def _editACL(self, **call):
    
        try:
            # check no. of args
            if not "ACL_info" in call["arguments"] or \
                not len(call["arguments"]["ACL_info"]) > 0:
                raise parse_exception_wrong_no_arguments(
                        "incorrect number of arguments")
            
            # TO DO
            # check permission
            
            #[{"ACL_type": "dir", "directory": <dir ACL Id>, 
            #"IO_privilege": <priv>, "computing_privilege": <priv>, 
            #"recursive": "0/1",
            #"target_type": "user/group", "target_Id": <Id>}, 
            #{"ACL_type": "antn", "annotation_Id": <antn ACL Id>, 
            #"privilege": <priv>, 
            #"target_type": "user/group", "target_Id": <Id>}, 
            #...]
            result = self.db_operator.editACL(call["arguments"]["ACL_info"])
                
            status = True
        
        except Exception as e:
            print("Exception: " + str(e.message))
            status = False
            result = {}
            # log failure
            #self.log_file_prefix + time + user + command + process
            
        # end of try
                
        return_value = {\
            "status": status,
            "result": result
        }
                
        #print(str(return_value))
        return json.dumps(return_value)
    
    # end of function _editACL
    
    ##
    # @brief    Remove an ACL entry for a path/annotation.
    #
    def _removeACL(self, **call):
    
        try:
            # check no. of args
            if not "ACL_info" in call["arguments"] or \
                not len(call["arguments"]["ACL_info"]) > 0:
                raise parse_exception_wrong_no_arguments(
                        "incorrect number of arguments")
            
            # TO DO
            # check permission
            
            #[{"ACL_type": "dir", "directory": <dir_ACL Id>,
            #"target_type": "user/group", "target_Id": <Id>}, 
            #{"ACL_type": "antn", "annotation": <antn_ACL Id>,
            #"target_type": "user/group", "target_Id": <Id>}, 
            #...]
            result = self.db_operator.removeACL(call["arguments"]["ACL_info"])
                
            status = True
        
        except Exception as e:
            traceback.print_exc()
            print("Exception: " + e.message)
            status = False
            result = {}
            # log failure
            #self.log_file_prefix + time + user + command + process
            
        # end of try
                
        return_value = {\
            "status": status,
            "result": result
        }
                
        #print(str(return_value))
        return json.dumps(return_value)
    
    # end of function _removeACL

# end of class BICLSU_Command_Service

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--host', dest='IP_or_FQDN', required=True)
    parser.add_argument('--port', dest='port', type=int, required=True)
    parser.add_argument('--db-path', dest='DB_path', required=True)
    parser.add_argument('--coordinator-url', 
        dest='coordinator_url', required=True)
    parser.add_argument('--iRODS-admin-resc', 
        dest='iRODS_admin_resc_path',
        help="<iRODS administration resc>")
    parser.add_argument('--portal-admin-resc', 
        dest='portal_admin_resc_path',
        help="<web portal administration resc>")
    parser.add_argument('--trans-admin-resc', 
        dest='transfer_admin_resc_path',
        help="<transfer administration resc>")
    parser.add_argument('--job-script-parsing', nargs=6,
        help="<uploaded script resc> "
            "<to GUI skeleton> <valid tagged script repo> "
            "<converted script repo> <to script> "
            "<script download resc>")
    parser.add_argument('--log-prefix', dest='log_path_prefix')
    parser.add_argument('--ssh-key-pair', dest='SSH_key_pair_info', nargs=3, 
        help="<prefix> <pri> <pub>")
    parser.add_argument('--ssh-cert', dest='SSH_cert_info', nargs=3,
        help="<CA pub> <prefix> <cert>")
    
    args = parser.parse_args()

    job_script_parsing = args.job_script_parsing
    log_path_prefix = args.log_path_prefix
    SSH_key_pair_info = args.SSH_key_pair_info
    SSH_cert_info = args.SSH_cert_info

    #DNS lookup
    #address_list = \
    #    socket.getaddrinfo(args.IP_or_FQDN, args.port, 
    #        0, 0, socket.IPPROTO_TCP)
    #print("addr ls: " + str(address_list))

    # Create server
    server = \
        threadedSimpleXMLRPCServer( 
            (args.IP_or_FQDN, args.port), 
            requestHandler=RequestHandler, 
            allow_none=True
        )
   
    server.register_function(
        BICLSU_Command_Service(
            args.DB_path,
            args.coordinator_url,
            args.transfer_admin_resc_path,
            args.iRODS_admin_resc_path,
            args.portal_admin_resc_path,
            job_script_parsing,
            log_path_prefix,
            SSH_key_pair_info,
            SSH_cert_info
        ).launch, 'launch')
    
    # Run the server's main loop
    server.serve_forever()
