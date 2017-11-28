from __future__ import print_function

from POSIX_Utilities import *

class BSD_Utilities(POSIX_Utilities):

    def __init__(self):
    
        #super(self.__class__, self).__init__()
        super(BSD_Utilities, self).__init__()
    
        print("init BSD_Utilities")
    
    # end of function __init__
    
    def __del__(self):
    
        print("clean BSD_Utilities")
    
    # end of function __del__
    
    def list(self, parent_path_escaped, list_type):
    
        #command_line = "'"
        command_line = ""
        # set parent directory
        command_line += "parent_path=\"" + parent_path_escaped + "\" && echo \"BIC-LSU 01\" && "
        # list immediate children paths
        # display type/path/size/modification time
        file_entry_generation = "test -f \"$1\"; then stat -f \"f/%N/%z/%Sm\" -t \"%Y-%m-%d %H:%M\" \"$1\";"
        directory_entry_generation = "test -d \"$1\"; then stat -f \"d/%N/%z/%Sm\" -t \"%Y-%m-%d %H:%M\" \"$1\";"
        if list_type == "all":
            entry_generation = "if "
            entry_generation += file_entry_generation
            entry_generation += " elif "
            entry_generation += directory_entry_generation
            entry_generation += " fi"
        elif list_type == "directory":
            entry_generation = "if "
            entry_generation += directory_entry_generation
            entry_generation += " fi"
        
        command_line += "child_paths=$(find \"${parent_path}\" -maxdepth 1 -exec sh -c '" + entry_generation + "' _ {} \\; | tail -n +2) && echo \"BIC-LSU 02\" && "
        # replace starting - with d
        # trim parent path prefix
        # sort
        command_line += "escaped_parent_path=$(echo \"${parent_path}\" | sed \"s/\\//\\\\\\\\\\//g\") && echo \"BIC-LSU 03\" && "
        command_line += "child_path_list=$(echo \"${child_paths}\" | sed \"s/^-/f/1;s/\\/${escaped_parent_path}//1\" | awk -F\"/\" \"{print substr(\\$1,1,1)\\\"/\\\"\\$2\\\"/\\\"\\$3\\\"/\\\"\\$4}\" | sort) && echo \"BIC-LSU 04\" && "
        command_line += "echo \"BIC-LSU succeeded\" && "
        command_line += "echo \"${child_path_list}\""
        #command_line += "'"
        
        return command_line
    
    # end of function list
    
    def getSize(self, path_escaped):
        
        command_line = "du "
        command_line += "-B 1048576"
        # TO DO
        #retrieve only the size number string
        
        return command_line
        
    # end of function getSize

# end of class File_Operation

if __name__ == "__main__":

    obj = BSD_Utilities()
    
    print("unit test: " + type(obj).__name__)
    
# end of function __main__
