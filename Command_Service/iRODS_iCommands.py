from __future__ import print_function

from UNIX_like_File_Interface import *

class iRODS_iCommands(UNIX_like_File_Interface):

    def __init__(self, shell, utilities, session):
    
        #super(self.__class__, self).__init__(shell, utilities, session)
        super(iRODS_iCommands, self).__init__(shell, utilities, session)
    
        print("init iRODS_iCommands")
    
    # end of function __init__
    
    def __del__(self):
    
        print("clean iRODS iCommands")
    
    # end of function __del__
    
    def list(self, path):
        pass
    # end of function list (implementation of the abstract function)
    
    def rename(self, old_path, new_path):
        pass
    # end of abstract function rename (implementation of the abstract function)
    
    def remove(self, path):
        pass
    # end of abstract function remove (implementation of the abstract function)
    
    def copy(self, origin_path, copy_path):
        pass
    # end of abstract function copy (implementation of the abstract function)
    
    def move(self, source_path, destination_path):
        pass
    # end of abstract function move (implementation of the abstract function)
    
    def create(self, path):
        pass
    # end of abstract function create
    # (implementation of the abstract function)
    
    def getACL(self, path):
        pass
    # end of abstract function getACL
    # (implementation of the abstract function)
    
    def setACL(self, 
        user, privilege, path,
        recursive=False, admin_mode=False, launch=True):
        
        if not privilege in \
            ["null", "read", "write", "own"]:
            return_value["status"] = False
            return_value["message"] = "Invalid privilege."
        
        command_line = "ichmod"
        
    # end of abstract function setACL
    # (implementation of the abstract function)
    
    def modifyACL(self, 
        user_or_group, privilege, path,
        recursive=False, admin_mode=False, launch=True):
        
        if not privilege in \
            ["null", "read", "write", "own"]:
            return_value["status"] = False
            return_value["message"] = "Invalid privilege."
        
        command_line = "ichmod"
        options = ""
        if recursive:
            options += "r"
        if admin_mode:
            options += "M"
        if options:
            command_line += " -" + options
        command_line += " " + privilege + " " + user_or_group + " " + path
        #print("cmd: " + str(command_line))
        
        if not launch:
            return_value["status"] = True
            return_value["command line"] = command_line
            return json.dumps(return_value)
            
        result = \
            self.launch(command_line)
            
        if not result["exit status"] == 0:
            # remote error
            # find out where the failure is
            # recover if necessary
            return_value["status"] = False
            return_value["message"] = "modify ACL error"
        else:
            return_value["status"] = True
        
        return json.dumps(return_value)
        
    # end of abstract function modifyACL
    # (implementation of the abstract function)
    
    def removeACL(self, 
        user, privilege, path,
        recursive=False, admin_mode=False, launch=True):
        
        if not privilege in \
            ["null", "read", "write", "own"]:
            return_value["status"] = False
            return_value["message"] = "Invalid privilege."
        
        command_line = "ichmod"
        
    # end of abstract function removeACL
    # (implementation of the abstract function)
    
    # iRODS specific functions
    
    def register(self, 
        admin_user, owner, 
        local_path, logical_path, launch=True):
        # must run on resource server as "irods"
        
        # add own privilege for admin to parent
        # register
        # add own privilege to user
        
        return_value = {}
        
        is_directory = False
        if local_path[len(local_path) - 1] == '/' and \
            logical_path[len(logical_path) - 1] == '/':
            is_directory = True
            local_path_processed = local_path[:len(local_path)]
            logical_path_processed = logical_path[:len(logical_path)]
        elif not local_path[len(local_path) - 1] == '/' and \
            not logical_path[len(logical_path) - 1] == '/':
            pass
        else:
            return_value["status"] = False
            return_value["message"] = \
                "Logical and physical path types not compatible."
            return return_value
            
        # TO DO:
        # escape paths
        
        command_line = ""
        
        result = \
            self.modifyACL(admin_user, "own", local_path_parent,
                recursive=True, admin_mode=True, launch=False)
        command_line += json.loads(result)["command line"]
        command_line += " && echo \"BIC-LSU 01\" && "
        
        command_line += "ireg "
        if is_directory:
            command_line += "-C "
        command_line += "" + \
            local_path_processed + " " + logical_path_processed + \
            " && echo \"BIC-LSU 02\" && "
        
        result = \
            self.modifyACL(owner, "own", local_path,
                recursive=True, admin_mode=True, launch=False)
        command_line += json.loads(result)["command line"]
        command_line += " && echo \"BIC-LSU 03\" && "
        
        command_line += "echo \"BIC-LSU succeeded\""
        
        if not launch:
            return_value["status"] = True
            return_value["command line"] = command_line
            return json.dumps(return_value)
            
        result = \
            self.launch(command_line)
            
        if not result["exit status"] == 0:
            # remote error
            # find out where the failure is
            # recover if necessary
            return_value["status"] = False
            return_value["message"] = "register error"
        else:
            return_value["status"] = True
        
        return json.dumps(return_value)
        
    # end of function register

# end of class iRODS_iCommands

if __name__ == "__main__":

    obj = iRODS_iCommands("POSIX SHELL", "BSD UTILITIES 8", "SSH SESSION")
    
    print("unit test: " + type(obj).__name__)
    
# end of function __main__
