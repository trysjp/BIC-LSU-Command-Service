from __future__ import print_function

from UNIX_like_Job_Control import *

class LSF(UNIX_like_Job_Control):

    def __init__(self, shell, utilities, session):
    
        #super(self.__class__, self).__init__(shell, utilities, session)
        super(LSF, self).__init__(shell, utilities, session)
    
        print("init LSF")
    
    # end of function __init__
    
    def __del__(self):
    
        print("clean LSF")
    
    # end of function __del__
    
    def submitJob(self, script_path, launch=True):
        
        command_line = "bsub < "+ script_path
        
        return_value = {}
        
        if not launch:
            return_value["status"] = True
            return_value["command"] = command_line
            return json.dumps(return_value)
            
        # create a temp file
        # transfer a submittable script
        result = \
            self.launch(command_line)
            
        # parse result
            
        return json.dumps(return_value)
        
    # end of function submitJob (implementation of the abstract function)
    
    def deleteJob(self, job_identifier, launch=True):
        
        command_line = "bkill " + job_identifier
        
        return_value = {}
        
        if not launch:
            return_value["status"] = True
            return_value["command"] = command_line
            return json.dumps(return_value)
            
        result = \
            self.launch(command_line)
            
        # parse result
        
        return json.dumps(return_value)
        
    # end of function deleteJob (implementation of the abstract function)
    
    def showJobStatus(self, user_account):
        
        command_line = "bjobs -u " + user_account
        
        return_value = {}
        
        if not launch:
            return_value["status"] = True
            return_value["command"] = command_line
            return json.dumps(return_value)
            
        result = \
            self.launch(command_line)
            
        # parse result
        
        return json.dumps(return_value)
        
    # end of function showJobStatus
    # (implementation of the abstract function)

# end of class LSF

if __name__ == "__main__":

    obj = LSF("KORN SHELL", "GNU UTILITIES 8", "SSH SESSION")
    
    print("unit test: " + type(obj).__name__)
    
# end of function __main__
