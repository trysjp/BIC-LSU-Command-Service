from __future__ import print_function

from UNIX_like_Utility_Tool import *

class POSIX_Utilities(UNIX_like_Utility_Tool):

    def __init__(self):
    
        #super(self.__class__, self).__init__()
        super(POSIX_Utilities, self).__init__()
    
        print("init POSIX_Utilities")
    
    # end of function __init__
    
    def __del__(self):
    
        print("clean POSIX_Utilities")
    
    # end of function __del__
    
    def remove(self, path):
    
        command_line = "rm -rf \"" + path + "\""
    
        return command_line
    
    # end of function remove
    
    def rename(self, old_path, new_path):
    
        command_line = "mv -n \"" + old_path + "\" \"" + new_path + "\""
    
        return command_line
    
    # end of function rename
    
    def create(self, path):
    
        command_line = "mkdir \"" + path + "\""
        
        return command_line
    
    # end of function create
    
    def copy(self, origin_path, copy_path):
    
        command_line = "cp -rn \"" + origin_path + "\" \"" + copy_path + "\""
        
        return command_line
    
    # end of function copy
    
    def changeMode(self, path, operation, privileges, recursive=False):
    
        command_line = "chmod "
        
        if recursive:
            command_line += " -R"
            
        command_line += " u"
        if operation == "add":
            command_line += "+"
        elif operation == "remove":
            command_line += "-"
        elif operation == "set":
            command_line += "="
        for one_privilege in privileges:
            if one_privilege == "read":
                command_line += "r"
            elif one_privilege == "write":
                command_line += "w"
            elif one_privilege == "execute":
                command_line += "x"
            elif one_privilege == "search":
                command_line += "X"
            
        command_line += " " + path
        
        return command_line
    
    # end of function changeMOde

# end of class File_Operation

if __name__ == "__main__":

    obj = POSIX_Utilities()
    
    print("unit test: " + type(obj).__name__)
    
# end of function __main__
