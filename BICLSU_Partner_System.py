from __future__ import print_function

from Command_Service import *
#from BICLSU_DB_Operator import *

#from SimpleXMLRPCServer import SimpleXMLRPCServer
#from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler
#from SocketServer import ThreadingMixIn
#from SocketServer import TCPServer
#from SimpleXMLRPCServer import SimpleXMLRPCDispatcher

#import argparse
import time
import random
#import xmlrpclib
#import json
#import ssl
#import socket
#import thread
import os
import subprocess
import tempfile
import string

class PartnerSystem(object):

    def __init__(self, partner_system_information):
        self.partner_system_information = partner_system_information
        #print("par sys info: " + str(self.partner_system_information))
        
    # end of function __init__
    
    ##
    # @brief            Add parameters specific to the target operation.
    #
    # @param parameter  A dictionary of parameter names and values.
    def addOperationSpecificParameters(self, parameter):
    
        self.operation_parameter = parameter
    
    # end of function addOperationSpecificParameters
    
    def setListType(self, list_type):
    
        self.list_type = list_type
    
    # end of function setListType
    
    def getListType(self):
    
        return self.list_type
    
    # end of function getListType
        
    def getResourceId(self):
    
        return self.partner_system_information\
                ["host information"]["resource Id"]
    
    # end of function getResourceId
        
    def getResourceServerType(self):
    
        return self.partner_system_information\
                ["host information"]["resource server type"]
    
    # end of function getResourceServerType
    
    def getPrivateUser(self):
    
        return self.partner_system_information\
                ["host information"]["private user"]
    
    # end of function getPrivateUser
    
    def getRelativePaths(self):
    
        return self.partner_system_information["relative paths"]
    
    # end of function getRelativePaths
        
    def getLocalPaths(self):
    
        return self.partner_system_information["local paths"]
    
    # end of function _getLocalPath
        
    def addLocalPaths(self, path_list, index=-1):
    
        if index >= 0 and \
            index <= len(self.partner_system_information["local paths"]):
            current_index = index
            for one_path in path_list:
                self.partner_system_information["local paths"].\
                    insert(current_index, one_path)
                current_index = current_index + 1
        else:
            self.partner_system_information["local paths"].extend(path_list)
    
    # end of function _addLocalPath
    
    def setPrivateUser(self, user):
    
        self.partner_system_information\
            ["host information"]["private user"] = str(user)
    
    #end of function setPrivateUser
    
    def getTransferApplication(self):
    
        if "transfer application" in \
            self.partner_system_information["host information"]:
            return self.partner_system_information\
                    ["host information"]["transfer application"]
        else:
            return None
    
    # end of function _getTransferApplication
    
    ##
    # @brief
    #
    # @param    application The application ID.
    #
    # @param    GUI_input   The list of JSON obejct in 
    #                       Argument <input from GUI path> for 
    #                       BICLSU_GUIInput_to_JobScript.py.
    #
    def addApplicationInformation(self, 
        application, 
        GUI_input, 
        converted_script_path = None):
    
        self.application = application
        self.application_input = GUI_input
        self.application_converted_script_path = converted_script_path
    
    # end of function addApplicationInput
    
    def prepareDataTransfer(self,
        transfer_type,
        transfer_application = None,
        transfer_direction = None,
        role = None,
        source_information = None,
        destination_information = None,
        server_ports = None,
        TAN_information = None):
        
        self.transfer_type = transfer_type
        if transfer_type in \
            ["3rd-party", "client-server"]:
            self.transfer_application = transfer_application
            self.transfer_direction = transfer_direction
            self.transfer_server_ports = server_ports
            self.transfer_source_information = source_information
            self.transfer_destination_information = destination_information
            self.TAN_information = TAN_information
            if transfer_type == "client-server":
                self.transfer_role = role
    
    # end of function prepareDataTransfer
    
    def sendSmallFileToCommandService(self, local_path, cmd_svc_path):
    
        authentication_information = \
            self._prepareAuthentication()
        #print("auth info: " + str(authentication_information))
            
        session_information = \
            self._prepareSession(
                authentication_information,
                "regular", 
                "IPv4",
                (1000, 40000))
        #print("session info: " + str(session_information))
        
        session_information.transferSmallFile(
            cmd_svc_path, local_path, send = False, launch = True)
        
        return_value = {}
        
        return_value["status"] = True
        return_value["message"] = None
        
        return return_value
    
    # end of function sendSmallFileToCommandService
        
    def controlJob(self, operation):
        #self.authenticate()
        #print(str(self.partner_system_information\
        #    ["host information"]["service"]["scheduling"]))
        
        authentication_information = \
            self._prepareAuthentication()
        #print("auth info: " + str(authentication_information))
            
        session_information = \
            self._prepareSession(
                authentication_information,
                "regular", 
                "IPv4",
                (1000, 40000))
        #print("session info: " + str(session_information))
                
        job_control_interface = \
            self._prepareJobControl(
                authentication_information, 
                session_information)
                
        if operation == "schedule":
        
            self.operation_parameter["session"] = session_information
        
            return_value = \
                job_control_interface.submitJob(
                    self.operation_parameter, launch=True)
                
        elif operation == "monitor":
            
            return_value = \
                job_control_interface.showJobStatus(
                    self.operation_parameter, launch=True)
            
        elif operation == "abort":
        
            return_value = \
                job_control_interface.deleteJob(
                    self.operation_parameter, launch=True)
                        
        else:
            return_value["status"] = False
            return_value["message"] = \
                "Job Control Operation \"" + operation + "\" is not supported."
        
        #print("ctl job return: " + str(return_value))
        return return_value
        
    # end of function controlJob
        
    def launchFileOperation(self, operation):
        
        return_value = {}
        
        if operation in \
            ["list", "rename", "create", "remove"]:
            authentication_information = \
                self._prepareAuthentication()
            print("auth info: " + str(authentication_information))
             
            session_information = \
                self._prepareSession(
                    authentication_information,
                    "regular", 
                    "IPv4",
                    (1000, 40000))
            #print("session info: " + str(session_information))
                    
            file_operation_interface = \
                self._prepareFileOperation(
                    "storage",
                    authentication_information, 
                    session_information)
            print("file operation info: " + str(file_operation_interface))
            
            if operation == "list":
                    
                result = \
                    file_operation_interface.list(
                        str(self.partner_system_information["local paths"][0]),
                        list_type=self.list_type)
                        
                print("result in launchFileOperation.list: " + str(result))
                
                return_value = result
                
            elif operation == "create":
            
                result = \
                    file_operation_interface.create(
                        str(self.partner_system_information["local paths"][0]))
                        
                #print("result in launchFileOperation: " + str(result))
                        
                return_value = result
           
            elif operation == "rename":
            
                result = \
                    file_operation_interface.rename(
                        str(self.partner_system_information["local paths"][0]),
                        str(self.partner_system_information["local paths"][1]))
                        
                #print("result in launchFileOperation: " + str(result))
                        
                return_value = result
                
            elif operation == "remove":
            
                result = \
                    file_operation_interface.remove(
                        str(self.partner_system_information["local paths"][0]))
                        
                #print("result in launchFileOperation: " + str(result))
                        
                return_value = result
                
        elif operation in \
            ["copy"]:
            #if local copy
            #else if bbcp 3rd-party
            #else if fdt server
            #else if fdt client
        
            if self.transfer_type == "local":
                #print("local copy")
                #print("from: " + str(self.partner_system_information\
                #                    ["local paths"][0]))
                #print("to: " + str(self.partner_system_information\
                #                    ["local paths"][1]))
                
                authentication_information = \
                    self._prepareAuthentication()
                #print("auth info: " + str(authentication_information))
                 
                session_information = \
                    self._prepareSession(
                        authentication_information,
                        "regular", 
                        "IPv4",
                        (1000, 10000))
                #print("session info: " + str(session_information))
                        
                file_operation_interface = \
                    self._prepareFileOperation(
                        authentication_information, 
                        session_information,
                        "storage")
                #print("file operation info: ," + str(file_operation_interface))
                
                result = \
                    file_operation_interface.copy(
                        str(self.partner_system_information["local paths"][0]),
                        str(self.partner_system_information["local paths"][1]))
                        
                return_value = json.loads(result)
                
            elif self.transfer_type == "3rd-party":
                #print("trans app: " + str(self.transfer_application))
                #print("from: " + str(self.transfer_source_information.\
                #                    getLocalPaths()[0]))
                #print("to: " + str(self.transfer_destination_information.\
                #                    getLocalPaths()[0]))
                
                util = utility.utility()
                
                # 3rd-party copy thru file_operation_interface
                if self.transfer_application[0]["name"] == "BBCP":

                    # bbcp path on 3rd-party host
                    command_line = str(self.transfer_application[2]["path"])
                    
                    # synthesizes parameters
                    buffer_size = None
                    direct_IO = None
                    TCP_window = None
                    # source parameters
                    parameter_segments = \
                        self.transfer_application[0]["read parameters"].split()
                    index = 0
                    while index < len(parameter_segments):
                        if parameter_segments[index][0] == '-':
                            if parameter_segments[index][1] == 'B':
                                buffer_size = \
                                    int(parameter_segments[index + 1]\
                                        [:len(parameter_segments[index + 1])-1])
                                #print("-B:" + str(buffer_size))
                                index += 2
                            elif parameter_segments[index][1] == 'u':
                                direct_IO = 's'
                                #print("-u:" + str(direct_IO))
                                index += 2
                            elif parameter_segments[index][1] == 'w':
                                TCP_window = \
                                    int(parameter_segments[index + 1]\
                                        [:len(parameter_segments[index + 1])-1])
                                #print("-w:" + str(TCP_window))
                                index += 2
                            else:
                                print("BBCP option not supported: " + \
                                    str(parameter_segments[index]))
                                index += 1
                    # target parameters            
                    parameter_segments = \
                        self.transfer_application[0]["write parameters"].split()
                    index = 0
                    while index < len(parameter_segments):
                        if parameter_segments[index][0] == '-':
                            if parameter_segments[index][1] == 'B':
                                buffer_size_target = \
                                    int(parameter_segments[index + 1]\
                                        [:len(parameter_segments[index + 1])-1])
                                if not buffer_size or \
                                    buffer_size_target > buffer_size:
                                    buffer_size = buffer_size_target
                                #print("-B:" + str(buffer_size))
                                index += 2
                            elif parameter_segments[index][1] == 'u':
                                if not direct_IO:
                                    direct_IO = 't'
                                else:
                                    direct_IO += 't'
                                #print("-u:" + str(direct_IO))
                                index += 2
                            elif parameter_segments[index][1] == 'w':
                                TCP_window_target = \
                                    int(parameter_segments[index + 1]\
                                        [:len(parameter_segments[index + 1])-1])
                                if not TCP_window or \
                                    TCP_window_target > TCP_window:
                                    TCP_window = TCP_window_target
                                #print("-w:" + str(TCP_window))
                                index += 2
                            else:
                                print("BBCP option not supported: " + \
                                    str(parameter_segments[index]))
                                index += 1
                    # parallelism
                    parallelism = int(self.transfer_application\
                                            [1]["write parallelism"])
                    command_line += " -s " + str(parallelism)

                    # recursive copy
                    command_line += " -r"

                    # omit existing paths at target
                    command_line += " -O"
                    
                    # follow symbolic links
                    # follow, keep, or ignore(default)
                    command_line += " -@ follow"
                    
                    # restrict transport layer ports for data connections
                    #command_line += " -Z " + \
                    #    str(self.transfer_server_ports[0]) + ":" + \
                    #    str(self.transfer_server_ports\
                    #        [len(self.transfer_server_ports) - 1])
                    
                    if buffer_size:
                        command_line += " -B " + str(buffer_size) + "m"
                    if direct_IO:
                        command_line += " -u " + str(direct_IO)
                    if TCP_window:
                        command_line += " -w " + str(TCP_window) + "m"
                    if self.TAN_information:
                        command_line += " -J " + \
                            str(self.TAN_information\
                                ["validation"]) + "," + \
                            str(self.TAN_information\
                                ["scheduler interface"]) + "," + \
                            str(self.TAN_information\
                                ["scheduler interface IP version"]) + "," + \
                            str(self.TAN_information\
                                ["scheduler port"]) + "," + \
                            str(self.TAN_information\
                                ["reporter"])
                    
                    # goes thru firewall
                    if not self.transfer_direction:
                        # target connects to source
                        command_line += " -z"
                    # else
                        # source connects to target
                    
                    command_line += " -S 'ssh %I -l %U %H"
                    
                    SSH_session_service_source = \
                        self.transfer_source_information.\
                            findSSHSessionService(
                                "BIC-LSU",
                                "IPv4",
                                [1000, None])
                    
                    # SSH server listening port on source
                    command_line += " -p " + \
                        str(SSH_session_service_source["port"])

                    # deactivate host key checking on source
                    command_line += " -o StrictHostKeyChecking=no"

                    # TO DO: specify private key to use
                    command_line += " -i PRI_KEY_SRC"

                    character_set = ['\'']
                    
                    # BBCP path on source
                    command_line += " " + \
                        util.escapeString(
                            str(self.transfer_application[0]["path"]),
                            character_set) + "'"
                    
                    command_line += " -T 'ssh %I -l %U %H"
                    
                    SSH_session_service_destination = \
                        self.transfer_destination_information.\
                            findSSHSessionService(
                                "BIC-LSU",
                                "IPv4",
                                [1000, None])
                    
                    # SSH server listening port on target
                    command_line += " -p " + \
                        str(SSH_session_service_destination["port"])
                    
                    # deactivate host key checking on target
                    command_line += " -o StrictHostKeyChecking=no"
                    
                    # TO DO: specify private key to use
                    command_line += " -i PRI_KEY_DST"
                    
                    # BBCP path on target
                    command_line += " " + \
                        util.escapeString(
                            str(self.transfer_application[1]["path"]),
                            character_set) + "'"
                    
                    # source url
                    command_line += " " + str(
                        self.transfer_source_information.\
                            getPrivateUser()) + "@" + \
                            str(SSH_session_service_source\
                                ["interface"]) + ":"
                    
                    # source path
                    command_line += str(
                        self.transfer_source_information.\
                            getLocalPaths()[0])
                
                    # target url
                    command_line += " " + str(
                        self.transfer_destination_information.\
                            getPrivateUser()) + "@" + \
                            str(SSH_session_service_destination\
                                ["interface"]) + ":"
                
                    # target path
                    command_line += str(
                        self.transfer_destination_information.\
                            getLocalPaths()[0])
                    
                print("cmd: " + str(command_line))
                
                authentication_information = \
                    self._prepareAuthentication()
                #print("auth info: " + str(authentication_information))
                 
                session_information = \
                    self._prepareSession(
                        authentication_information,
                        "regular", 
                        "IPv4",
                        (1000, 10000))
                #print("session info: " + str(session_information))
                
                result = \
                    session_information.executeCommandLine(command_line)
                
                if not result["exit status"] == 0:
                    return_value["status"] = False
                    return_value["message"] = "3rd-party remote copy error."
                else:
                    return_value["status"] = True
                
            elif self.transfer_type == "client-server":
                
                #print("trans app: " + str(self.transfer_application))
                #print("trans app path: " + str(self.transfer_application_path))
                #print("role: " + str(self.transfer_role))
                #print("src: " + str(self.transfer_source_information.\
                #                    getLocalPaths()[0]))
                #print("dst: " + str(self.transfer_destination_information.\
                #                    getLocalPaths()[0]))
                
                # client-server copy thru file_operation_interface
                if self.transfer_application["name"] == "FDT":
                    # max usable OS memory size
                    command_line = "java -XX:MaxDirectoryMemorySize=64m"
                    
                    # fdt path
                    command_line += " -jar " + \
                        str(self.transfer_application["path"])
                    
                    # server listening port
                    command_line += " -p " + str(self.transfer_server_ports[0])
                    
                    if self.transfer_role == "server":
                        # server mode
                        command_line += " -S"
                        
                        # parallelism
                        if self.transfer_direction:
                            command_line += " " + \
                                self.transfer_application["write parameters"]
                        else:
                            command_line += " " + \
                                self.transfer_application["read parameters"]
                                
                        # task-aware network parameters
                        if self.TAN_information:
                            command_line += " -TAN " + \
                                str(self.TAN_information\
                                    ["validation"]) + "," + \
                                str(self.TAN_information\
                                    ["scheduler interface"]) + "," + \
                                str(self.TAN_information\
                                    ["scheduler interface IP version"]) + "," +\
                                str(self.TAN_information\
                                    ["scheduler port"]) + "," + \
                                str(self.TAN_information\
                                    ["reporter"])
                                
                        # run in background
                        command_line += " &"
                    
                    elif self.transfer_role == "client":                           
                        if self.transfer_direction:
                            # client pushes and sets server IP address
                            command_line += " -c " + str(
                                self.transfer_destination_information.\
                                    findNetworkInterface(
                                    "BIC-LSU",
                                    "IPv4",
                                    [1000, None]))
                            command_line += " -P " + str(
                                self.transfer_application["read parallelism"])
                        else:
                            # client pulls from server
                            command_line += " -c " + str(
                                self.transfer_source_information.\
                                    findNetworkInterface(
                                    "BIC-LSU",
                                    "IPv4",
                                    [1000, None]))
                            command_line += " -pull"
                            command_line += " -P " + str(
                                self.transfer_application["write parallelism"])
                                
                        # task-aware network parameters
                        if self.TAN_information:
                            command_line += " -TAN " + \
                                str(self.TAN_information\
                                    ["validation"]) + "," + \
                                str(self.TAN_information\
                                    ["scheduler interface"]) + "," + \
                                str(self.TAN_information\
                                    ["scheduler interface IP version"]) + "," +\
                                str(self.TAN_information\
                                    ["scheduler port"]) + "," + \
                                str(self.TAN_information\
                                    ["reporter"])
                                    
                        # recursive copy
                        command_line += " -r"
                         
                        # destination directory path
                        command_line += " -d " + \
                            self.transfer_destination_information.\
                                getLocalPaths()[0]
                                    
                        # source path
                        command_line += " " + \
                            self.transfer_source_information.\
                                getLocalPaths()[0]
                        
                print("cmd: " + str(command_line))
                
                authentication_information = \
                    self._prepareAuthentication()
                #print("auth info: " + str(authentication_information))
                 
                session_information = \
                    self._prepareSession(
                        authentication_information,
                        "regular", 
                        "IPv4",
                        (1000, 10000))
                #print("session info: " + str(session_information))
                
                result = \
                    session_information.executeCommandLine(command_line)
                
                if not result["exit status"] == 0:
                    return_value["status"] = False
                    return_value["message"] = "client-server remote copy error."
                else:
                    return_value["status"] = True
        
        elif operation in \
            ["register"]:
            #file_operation_interface.register(
            #    [0], [1])
            pass
                   
        else:
            print("operation not implemented")
            return_value["status"] = False
            return_value["message"] = \
                "Operation " + operation + " not implemented"
        
        #print("cmd svc returned: " + str(return_value))
        return return_value
        
    # end of function launchFileOperation
    
    ##
    # @brief    Finds a list of interfaces which satisfy the requirements.
    #
    # @param    network_type_list   A priority list of network types.
    # @param    IP_version_list     A priority list of IP versions.
    # @param    bandwidth_range     A 2-tuple with min and max BW requirements.
    #
    # @return   A sorted list of filtered interfaces
    #           by net type, IP version, and BW.
    def findNetworkInterfaces_new(self,
        network_type_list,
        IP_version_list,
        bandwidth_range):
        
        candidate_interface_list = []
        
        for one_interface in \
            self.partner_system_information\
                ["host information"]["network interface"]:
                
            # filter with network type
            if not one_interface["network"] in network_type_list:
                continue
                
            # has network type
        
            # filter with IP version
            if "IPv6 interface" in IP_version_list and \
                one_interface["IPv6 interface"] == "":
                has_IPv6 = False
            if "IPv4 interface" in IP_version_list and \
                one_interface["IPv4 interface"] == "":
                # does not have IPv4
                if not has_IPv6:
                    continue

            # has IP version
        
            # filter with bandwidth constraints
            if bandwidth_range[0] and \
                one_interface["capacity"] < bandwidth_range[0]:
                continue
                
            # meet min BW
            
            if bandwidth_range[1] and \
                one_interface["capacity"] > bandwidth_range[1]:
                continue
            
            # meet max BW
            
            # meet all bandwidth constraints
            
            # add to candidate interface list
            candidate_interface_list.append(one_interface)

        # end of for
        
        # sort in a non-increasing order by net type, IP version, and BW
        for interface_1 in \
            candidate_interface_list[:len(candidate_interface_list)-1]:
            for interface_2 in \
                candidate_interface_list\
                    [candidate_interface_list.index(interface_1)+1:]:
                if _compareInterfaces(interface_1, interface_2) < 0:
                    # swap
                    pass
        
        return candidate_interface_list
        
    # end of function findNetworkInterface
    
    def _compareInterfaces(self,
        interface_1,
        interface_2,
        network_type_list,
        IP_version_list,
        bandwidth_range):
        
        # compare net type
        preference_1 = network_type_list.index(interface_1["network"])
        preference_2 = network_type_list.index(interface_2["network"])
        if preference_1 > preference_2:
            return 1
        elif preference_1 < preference_2:
            return -1
            
        # compare IP version
        for preferred_IP in IP_version_list:
            if not interface_1[preferred_IP] == "" and \
                interface_1[preferred_IP] == "":
                return 1
            elif interface_1[preferred_IP] == "" and \
                not interface_1[preferred_IP] == "":
                return -1
            
        # compare bandwidth
        if bandwidth_range[0]:
            # If both, use most BW.
            # If only min, use most BW.
            if interface_1["capacity"] > interface_2["capacity"]:
                return 1
            elif interface_1["capacity"] < interface_2["capacity"]:
                return -1
            else:
                return 0
        else: 
            # If only max, use least BW.
            # If none, use least BW.
            if interface_1["capacity"] < interface_2["capacity"]:
                return 1
            elif interface_1["capacity"] > interface_2["capacity"]:
                return -1
            else:
                return 0
    
    # end of function _compareInterfaces
    
    ##
    # @brief    Find 1 interface from each of src and dst partner systems 
    #           which best satisfy all preferences.
    #
    # @param    partner_system_src  The source partner system.
    # @param    partner_system_dst  The destination partner system.
    # @param    network_type_list   A priority list of network types.
    # @param    IP_version_list     A priority list of IP versions.
    # @param    bandwidth_range     A 2-tuple with min and max BW requirements.   
    #                               If none, meet none and use least BW.
    #                               If only max, meet max and use least BW.
    #                               If only min, meet min and use most BW.
    #                               If both, meet both and use most BW.
    #
    # @return   A list of 1 interface from each partner system.
    #
    def findBestCommonInterfacePair(self,
        partner_system_src,
        partner_system_dst,
        network_type_list,
        IP_version_list,
        bandwidth_range):
        # MOVE TO BICLSU_Command_Service
        
        list_1 = findNetworkInterface(
                    partner_system_src,
                    network_type_list,
                    IP_version_list,
                    bandwidth_range)
        list_2 = findNetworkInterface(
                    partner_system_dst,
                    network_type_list,
                    IP_version_list,
                    bandwidth_range)
        
        interface_pair = []
        found = False
        for interface_1 in list_1:
            for interface_2 in list_2:
                if interface_1["network"] == interface_2["network"]:
                    # has common net type
                    if not interface_1["IPv6 interface"] == "" and \
                        not interface_2["IPv6 interface"] == "":
                        # has common IP version, 6
                        interface_pair = \
                            [interface_1["IPv6 interface"],
                                interface_2["IPv6 interface"]]
                        found = True
                    elif not interface_1["IPv4 interface"] == "" and \
                        not interface_2["IPv4 interface"] == "":
                        # has common IP version, 4
                        interface_pair = \
                            [interface_1["IPv4 interface"],
                                interface_2["IPv4 interface"]]
                        found = True
                if found:
                    break
            # end of for interface_2
            if found:
                break
        # end of for interface_1
                        
        return interface_pair
        
    # end of function findBestCommonInterfacePair
    
    def findNetworkInterface(self,
        network_type,
        IP_version,
        interface_bandwidth_range):
        
        return_value = {}
        candidate_interface = None
        for one_interface in \
            self.partner_system_information\
                ["host information"]["network interface"]:
        
            if not network_type == "any" and \
                not one_interface["network"] == network_type:
                continue
        
            if not interface_bandwidth_range[0] or \
                (interface_bandwidth_range[0] and \
                one_interface["capacity"] >= interface_bandwidth_range[0]):
                    # min requirement met
                    pass
            else:
                # min requirement not met
                continue
                
            if not interface_bandwidth_range[1] or \
                (interface_bandwidth_range[1] and \
                one_interface["capacity"] <= interface_bandwidth_range[1]):
                    # max requirement met
                    pass
            else:
                # max requirement not met
                continue
        
            # prefers IPv6
            if (IP_version == "IPv6" or \
                IP_version == "any") and \
                not one_interface["IPv6 interface"] == "":
                host_FQDN = one_interface["IPv6 interface"]
            elif (IP_version == "IPv4" or \
                IP_version == "any") and \
                not one_interface["IPv4 interface"] == "":
                host_FQDN = one_interface["IPv4 interface"]
            else:
                # IP version requirement not met
                continue
          
            # use interface with the least bandwidth
            if not candidate_interface or \
                ((candidate_interface["capacity"] - \
                interface_bandwidth_range[0]) > \
                (one_interface["capacity"] - \
                interface_bandwidth_range[0])):
                candidate_interface = one_interface
                candidate_host_FQDN = host_FQDN
                #print("use IF: " + str(candidate_interface))
              
        if not candidate_interface:
            print("no matched interface with required bandwidth.")
            return None
            
        return candidate_host_FQDN
    
    # end of function findNetworkInterface
    
    ##
    # @brief    Find the address of the SSH session service 
    #           which best fits the preferences.
    #
    # @param    network_type_list   A priority list of network types.
    # @param    IP_version_list     A priority list of IP versions.
    # @param    bandwidth_range     A 2-tuple with min and max BW requirements.   
    #                               If none, meet none and use least BW.
    #                               If only max, meet max and use least BW.
    #                               If only min, meet min and use most BW.
    #                               If both, meet both and use most BW.
    #
    # @return   A list of dictionary of "interface" and "port".
    #
    def findSSHSessionService_new(self,
        network_type_list,
        IP_version_list,
        bandwidth_range):
        
        # find interfaces which satisfy all preferences
        interface_list = findNetworkInterface(
                            network_type_list,
                            IP_version_list,
                            bandwidth_range)
                            
        #remove ones without SSH services
        #for one_interface in interface_list:
        #    if one_interface in 
        #        return_value.append((, ))
        
    # end of function findSSHSessionService_new
    
    def findSSHSessionService(self,
        network_type,
        IP_version,
        interface_bandwidth_range):
    
        return_value = {}
        candidate_interface = None
        
        print("OpenSSH Session Service: " + self.partner_system_information\
                ["host information"]["service"]\
                ["session"]["OpenSSH"].values()[0])
        
        for one_interface in \
            self.partner_system_information\
                ["host information"]["service"]\
                ["session"]["OpenSSH"].values()[0]:
        
            if not network_type == "any" and \
                not one_interface["network type"] == network_type:
                continue
        
            if not interface_bandwidth_range[0] or \
                (interface_bandwidth_range[0] and \
                one_interface["capacity"] >= interface_bandwidth_range[0]):
                    # min requirement met
                    pass
            else:
                # min requirement not met
                continue
                
            if not interface_bandwidth_range[1] or \
                (interface_bandwidth_range[1] and \
                one_interface["capacity"] <= interface_bandwidth_range[1]):
                    # max requirement met
                    pass
            else:
                # max requirement not met
                continue
        
            # prefers IPv6
            if (IP_version == "IPv6" or \
                IP_version == "any") and \
                not one_interface["IPv6 interface"] == "":
                host_FQDN = one_interface["IPv6 interface"]
            elif (IP_version == "IPv4" or \
                IP_version == "any") and \
                not one_interface["IPv4 interface"] == "":
                host_FQDN = one_interface["IPv4 interface"]
            else:
                # IP version requirement not met
                continue
          
            # if no max limit
            #   use interface with the most bandwidth 
            # if no min limit
            #   use interface with the least bandwidth
            # if has min limit
            #   use interface with the closet bandwidth to min limit
            if not candidate_interface or \
                (not interface_bandwidth_range[1] and \
                (candidate_interface["capacity"] < \
                one_interface["capacity"])) or \
                (not interface_bandwidth_range[0] and \
                (candidate_interface["capacity"] > \
                one_interface["capacity"])) or \
                (interface_bandwidth_range[0] and \
                ((candidate_interface["capacity"] - \
                interface_bandwidth_range[0]) > \
                (one_interface["capacity"] - \
                interface_bandwidth_range[0]))):
                candidate_interface = one_interface
                candidate_host_FQDN = host_FQDN
                #print("use IF: " + str(candidate_interface))
              
        if not candidate_interface:
            print("no matched interface with required bandwidth.")
            return None
        
        return_value["interface"] = candidate_host_FQDN
        return_value["port"] = candidate_interface["port"]
        
        #print("ssh srvc:" + str(return_value))
        
        return return_value
    
    # end of function findSSHSessionService
    
    ##
    # @brief    Find the authentication service of the current partner system.
    #
    # @return   All relative information of the authentication service.
    #
    def _prepareAuthentication(self,
        category = "regular"):
        # factory function for authentication
        
        if not category in \
            ["regular", "iRODS path registration"]:
            print("not supported service category " + category)
            return None
        
        return_value = {}
        
        return_value["account"] = \
            self.partner_system_information\
                ["host information"]\
                ["private user"]
        
        control_credential_category = \
            self.partner_system_information\
                ["host information"]\
                ["control credential"][0]["category"]
                
        #print("partner sys: " + str(self.partner_system_information))
        #print("ctrl credential cat: " + str(control_credential_category))
        
        if control_credential_category in \
            ["iRODS_password"] and not category == "iRODS path registration":
            # regular iRODS operation
            # iRODS path registration uses iCommands via SSH
            return_value["password"] = \
                self.partner_system_information\
                ["host information"]\
                ["iRODS password"]
        elif control_credential_category in \
            ["OpenSSH_PKI"]:
            return_value["private credential path"] = \
                self.partner_system_information\
                    ["control SSH key pair information"]["prefix path"] + \
                self.partner_system_information\
                    ["host information"]\
                    ["control credential"][0]["path"]
                    
            #print("pri key: " + str(return_value["private credential path"]))
            
            #if not self.partner_system_information\
            #    ["host information"]\
            #    ["control credential"][0]["passphrase"] == "":
            #    return_value["private credential passphrase"] = \
            #        self.partner_system_information\
            #        ["host information"]\
            #        ["control credential"][0]["passphrase"]
            
            #    print("passphrase: " + \
            #        str(return_value["private credential passphrase"]))
            
            return_value["private credential passphrase"] = \
                    self.partner_system_information\
                    ["host information"]\
                    ["control credential"][0]["passphrase"]
            
        elif control_credential_category in \
            ["OpenSSH_certificate"]:
            return_value["private credential path"] = \
                self.partner_system_information\
                    ["control SSH certificate information"]["prefix path"] + \
                self.partner_system_information\
                    ["host information"]\
                    ["control credential"][0]["path"]
                    
            #print("cert: " + str(return_value["private credential path"]))
                    
        elif control_credential_category in \
            ["Kerberos_MIT", "Kerberos_Heimdal"]:
            # TO DO: port the demo version to production
            pass
        else:
            print("control credential type not supported")
            
        return_value["category"] = control_credential_category
            
        server_version = \
            self.partner_system_information\
                ["host information"]\
                ["service"]\
                ["authentication"]\
                [control_credential_category].keys()[0]
        interface_list = \
            self.partner_system_information\
                ["host information"]\
                ["service"]\
                ["authentication"]\
                [control_credential_category]\
                [server_version]
                
        #print("interface list: " + str(interface_list))        
            
        return_value["interface"] = interface_list
        
        return return_value
    
    # end of function _prepareAuthentication
    
    def _prepareSession(self,
        authentication_information,
        network_type,
        IP_version,
        interface_bandwidth_range):
        # factory function for session
        
        candidate_interface = None
        for one_interface in authentication_information["interface"]:
        
            #print("if: " + str(one_interface))
        
            if not one_interface["network type"] == network_type:
                continue
        
            if not interface_bandwidth_range[0] or \
                (interface_bandwidth_range[0] and \
                one_interface["capacity"] >= interface_bandwidth_range[0]):
                    # min requirement met
                    minimum_bandwidth_requirement_met = True
            else:
                # min requirement not met
                minimum_bandwidth_requirement_met = False
                
            if not interface_bandwidth_range[1] or \
                (interface_bandwidth_range[1] and \
                one_interface["capacity"] <= interface_bandwidth_range[1]):
                    # max requirement met
                    maximum_bandwidth_requirement_met = True
            else:
                # max requirement not met
                maximum_bandwidth_requirement_met = False
        
            if minimum_bandwidth_requirement_met and \
                maximum_bandwidth_requirement_met:

                if not candidate_interface or \
                ((candidate_interface["capacity"] - \
                interface_bandwidth_range[0]) > \
                (one_interface["capacity"] - \
                interface_bandwidth_range[0])):
                    candidate_interface = one_interface
                    #print("use IF: " + str(candidate_interface))
            else:
                continue
              
        if not candidate_interface:
            print("no matched interface with required bandwidth.")
            return None
        
        host_FQDN = None
        if (IP_version == "IPv6" or \
            IP_version == "any") and \
            not candidate_interface["IPv6 interface"] == "":
            host_FQDN = candidate_interface["IPv6 interface"]
        elif (IP_version == "IPv4" or \
            IP_version == "any") and \
            not candidate_interface["IPv4 interface"] == "":
            host_FQDN = candidate_interface["IPv4 interface"]
        else:
            print("no matched interface with required IP version")
            return None 
            
        print("host: " + str(host_FQDN))
        
        if authentication_information["category"] in \
            ["iRODS_password"]:
            zone = "BICLSULSUBTR"
            # To avoid Exception "argument for 's' must be a string",
            # convert all parameters to strings.
            session = \
                iRODS_PythonClient_Session.iRODS_PythonClient_Session(
                    host = str(host_FQDN),
                    port = str(candidate_interface["port"]),
                    user = str(authentication_information["account"]),
                    password = str(authentication_information["password"]),
                    zone = str(zone))
            
        elif authentication_information["category"] in \
            ["OpenSSH_certificate", "OpenSSH_PKI"]:
            session = \
                SSH_Session.SSH_Session(
                    hostname = host_FQDN,
                    port = candidate_interface["port"],
                    username = authentication_information["account"],
                    key_filename = \
                        authentication_information["private credential path"],
                    password = \
                        authentication_information\
                            ["private credential passphrase"])
            
        return session
    
    # end of function _prepareSession
    
    def _prepareFileOperation(self,
        category,
        authentication_information,
        session_information):
        # factory function for file operation
        
        #TO DO: re-design the branching on category
        
        if not category in \
            ["storage", "iRODS path registration"]:
            print("not supported service category " + category)
            return None
        
        if category in \
            ["iRODS path registration"]:
            server_type = \
            self.partner_system_information\
                ["host information"]\
                ["service"]\
                ["registration"].keys()[0]
            #print("server type: " + str(server_type))
                
            if server_type == "iRODS_iCommands_registration":
                file_operation_interface = \
                    iRODS_iCommands.iRODS_iCommands(
                        "empty shell",
                        "empty utilities",
                        session_information)
        
        #server_type = \
        #    self.partner_system_information\
        #        ["host information"]\
        #        ["service"]\
        #        ["storage"].keys()[0]
        server_type = \
            self.partner_system_information\
            ["host information"]\
            ["resource server type"]
                
        if server_type in \
            ["GNU_Utilities", "BSD_Utilities"]:
            if server_type == "GNU_Utilities":
                file_operation_interface = \
                    UNIX_like_File_Utilities.UNIX_like_File_Utilities(
                        "empty shell",
                        "empty utilities",
                        session_information,
                        GNU_Utilities.GNU_Utilities())
            elif server_type == "BSD_Utilities":
                file_operation_interface = \
                    UNIX_like_File_Utilities.UNIX_like_File_Utilities(
                        "empty shell",
                        "empty utilities",
                        session_information,
                        BSD_Utilities.BSD_Utilities())
        elif server_type in \
            ["iRODS_iCommands"]:
            file_operation_interface = \
                iRODS_iCommands.iRODS_iCommands(
                    "empty shell",
                    "empty utilities",
                    session_information)
        elif server_type in \
            ["OpenStackSwift_REST"]:
            print("requests openstack swift REST")
        elif server_type in \
            ["WebHDFS"]:
            print("requests webhdfs REST")
        elif server_type in \
            ["iRODS"]:
            file_operation_interface = \
                iRODS_PythonClient.iRODS_PythonClient(session_information)
        else:
            print("not supported storage service")
            file_operation_interface = None
        # end of if server_type
        
        return file_operation_interface
    
    # end of function _prepareFileOperation
    
    def _prepareJobControl(self,
        authentication_information,
        session_information):
        # factory function for job control
        
        server_type = \
            self.partner_system_information\
                ["host information"]\
                ["service"]\
                ["scheduling"].keys()[0]
        #print("server type: " + str(server_type))
                
        if server_type in \
            ["TORQUE", "OpenPBS", "IBM_LSF", "OpenLava"]:
            if server_type == "TORQUE":
                job_control_interface = \
                    TORQUE.TORQUE(
                        "empty shell",
                        "empty utilities",
                        session_information,
                        self.partner_system_information\
                            ["job script parsing information"])
            elif server_type == "IBM_LSF":
                job_control_interface = \
                    IBM_LSF.IBM_LSF(
                        "empty shell",
                        "empty utilities",
                        session_information,
                        self.partner_system_information\
                            ["job script parsing information"])
            elif server_type == "OpenPBS":
                job_control_interface = \
                    OpenPBS.OpenPBS(
                        "empty shell",
                        "empty utilities",
                        session_information,
                        self.partner_system_information\
                            ["job script parsing information"])
            elif server_type == "OpenLava":
                job_control_interface = \
                    OpenLava.OpenLava(
                        "empty shell",
                        "empty utilities",
                        session_information,
                        self.partner_system_information\
                            ["job script parsing information"])
        elif server_type in \
            ["Oozie_REST"]:
            job_control_interface = \
                Oozie_REST.Oozie_REST(
                    "empty shell",
                    "empty utilities",
                    session_information)
        else:
            print("not supported scheduling service")
            job_control_interface = None
            
        return job_control_interface
    
    # end of function _prepareJobControl

# end of class PartnerSystem
