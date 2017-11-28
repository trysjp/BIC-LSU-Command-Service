from __future__ import print_function

import tempfile
import subprocess
import string
import json

from SSH_Session import *
from Job_Control import *

class UNIX_like_Job_Control(Job_Control):

    def __init__(self, shell, utilities, session, parsing_information):
    
        #super(self.__class__, self).__init__()
        super(UNIX_like_Job_Control, self).__init__()
        
        self.shell = shell
        self.utilities = utilities
        self.session = session
        self.job_script_parsing_information = parsing_information
        self.temp_path_pattern = "/tmp/BIC-LSU.XXXXXXXX"
    
        print("init UNIX_like_Job_Control.  " + \
            "Shell: " + str(self.shell) + ".  " + \
            "Utilities: " + str(self.utilities) + ".  " + \
            "Session: " + str(self.session))
    
    # end of function __init__
    
    def __del__(self):
    
        print("clean UNIX_like_Job_Control")
    
    # end of function __del__
    
    ##
    # @brief                Prepare a submittable job script with user inputs
    #                       on the computing facility.
    #
    # @param    parameter   A dictionary of all required
    #                       (1) intermediate job batch scripts.
    #                       (2) parsers.
    #                       (3) user inputs from GUI.
    #                       (4) established session to the computing facility.
    #
    # @return               Path to the submittable job batch script 
    #                       if succeeds.  None if fails.
    def prepareJobBatchScript(self, parameter, launch = True): 
        
        #generate final job script
        GUI_input_file = tempfile.NamedTemporaryFile(\
            mode = "w+b", prefix = "GUI_", delete = True)
        GUI_input_file_path = GUI_input_file.name
        #print("GUI path: " + str(GUI_input_file_path))
        #print("GUI in: " + str(parameter["GUI input"]))
        GUI_input_file.write(json.dumps(parameter["GUI input"]))
        GUI_input_file.flush()
        #make file ready for reading
        GUI_input_file.seek(0)
        #print("gui con: " + str(GUI_input_file.read()))
        #GUI_input_file.seek(0)
        #print("conv p: " + \
        #    str(parameter["converted script path"]))
        submittable_script = \
            subprocess.check_output(
                ["python", 
                parameter["combining parser path"],
                parameter["converted script path"],
                GUI_input_file_path])
        #close(delete) the temp GUI input file
        GUI_input_file.close()
        #print("sub script: " + str(submittable_script))
        submittable_script_file = tempfile.NamedTemporaryFile(
            mode = "w+b", prefix = "submittable_", delete = True)
        submittable_script_file_path = submittable_script_file.name
        #print("sub path: " + str(submittable_script_file_path))
        submittable_script_file.write(submittable_script)
        submittable_script_file.flush()
        #make file ready for reading
        submittable_script_file.seek(0)
        
        #transfer final job script to computing facility
        command_line = "mktemp"
        return_value = parameter["session"].executeCommandLine(command_line)
        #print("f p: " + str(return_value["stdout"] + " " + \
        #    str(len(return_value["stdout"]))))
        #remove EOF
        remote_submittable_script_file_path = \
            return_value["stdout"][:len(return_value["stdout"]) - 1]
        #print("r sub path: " + str(remote_submittable_script_file_path))
        result = parameter["session"].transferSmallFile(
            submittable_script_file_path, remote_submittable_script_file_path, 
            send = True, launch = launch)
        #close(delete) the temp submittable script
        submittable_script_file.close()
                
        if not result["status"]:
            print("Transferring submittable job batch script failed. " + \
                str(result))
            return None
            
        return remote_submittable_script_file_path
        
    # end of function prepareJobBatchScript
    
    def launch(self, command_line): 
        
        print("run Command " + command_line + \
            " via Session " + str(self.session))
        
        return self.session.executeCommandLine(command_line)
        
    # end of function launch

# end of class UNIX_like_Job_Control

if __name__ == "__main__":

    obj = UNIX_like_Job_Control()
    
    print("unit test: " + type(obj).__name__)
    
# end of function __main__
