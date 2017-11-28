from __future__ import print_function

from PBS import *

import json
import os

class TORQUE(PBS):

    def __init__(self, shell, utilities, session, parsing_information):
    
        #super(self.__class__, self).__init__(shell, utilities, session)
        super(TORQUE, self).__init__(
            shell, utilities, session, parsing_information)
    
        print("init TORQUE")
    
    # end of function __init__
    
    def __del__(self):
    
        print("clean TORQUE")
    
    # end of function __del__
    
    def submitJob(self, parameter, launch=True):
    
        #prepare submittable job script on computing facility
        script_path = \
            self.prepareJobBatchScript(parameter, launch)
    
        command_line = "qsub \""+ script_path + "\""
        
        return_value = {}
        
        if not launch:
            return_value["status"] = True
            return_value["command"] = command_line
            return return_value
            
        result = \
            self.launch(command_line)
            
        if not result["status"]:
            return_value["status"] = False
            return_value["message"] = result["message"]
            return return_value
            
        # parse result
        if not result["exit status"] == 0:
            return_value["status"] = False
            return_value["message"] = \
                "Output Message: " + \
                str(result["stdout"].lstrip("\r\n").rstrip("\r\n")) + \
                os.linesep + \
                "Error Message: " + \
                str(result["stderr"].lstrip("\r\n").rstrip("\r\n")) + \
                os.linesep
            return return_value
            
        return_value["status"] = True
        return_value["job id"] = result["stdout"].lstrip("\r\n").rstrip("\r\n")
            
        return return_value
        
    # end of function submitJob (implementation of the abstract function)
    
    def deleteJob(self, job_Id, launch=True):
    
        # TO DO
        # delete multiple jobs
    
        command_line = "qdel \"" + job_Id + "\""
        
        return_value = {}
        
        if not launch:
            return_value["status"] = True
            return_value["command"] = command_line
            return json.dumps(return_value)
            
        result = \
            self.launch(command_line)
            
        # parse result
        
        return return_value
        
    # end of function deleteJob (implementation of the abstract function)
    
    def showJobStatus(self, parameter, launch=True):
        
        command_line = \
            "qstat -u \"" + parameter["user"] + "\" | " + \
            "grep --regex=\"" + parameter["user"] + "\""
        
        return_value = {}
        
        if not launch:
            return_value["status"] = True
            return_value["command"] = command_line
            return return_value
            
        result = \
            self.launch(command_line)

        # parse result
        status_mapping = {
            "R": "Running",
            "E": "Exiting",
            "H": "Held",
            "Q": "Queued",
            "T": "Moved",
            "W": "Waiting",
            "S": "Suspended"}
        
        job_list = []
        output_lines = result["stdout"].split('\n')
        #print("out ln: " + str(output_lines))
        for one_line in output_lines:
            # only process a non-empty line
            if one_line:
                segments = one_line.split()
                
                job_status = status_mapping[segments[9]]
                
                job_list.append({
                    "id": str(segments[0]),
                    "name": str(segments[3]),
                    "status": str(job_status)})
                
        return_value["job_list"] = job_list
        
        return_value["status"] = True
        
        #print("ret val: " + str(return_value))
        return return_value
        
    # end of function showJobStatus (implementation of the abstract function)
    
    def estimateJobStartingTime(self, job_Id, launch=True):
    
        command_line = "showstart \"" + job_Id + "\""
        
        return_value = {}
        
        if not launch:
            return_value["status"] = True
            return_value["command"] = command_line
            return json.dumps(return_value)
            
        result = \
            self.launch(command_line)
            
        # parse result
        
        return return_value
        
    # end of function estimateJobStartingTime
    # (implementation of the abstract function)

# end of class TORQUE

if __name__ == "__main__":

    obj = TORQUE("KORN SHELL", "GNU UTILITIES 8", "SSH SESSION")
    
    print("unit test: " + type(obj).__name__)
    
# end of function __main__
