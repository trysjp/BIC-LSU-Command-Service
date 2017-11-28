from __future__ import print_function

import json

from UNIX_like_File_Interface import *
from utility import *

class UNIX_like_File_Utilities(UNIX_like_File_Interface):

    def __init__(self, shell, utilities, session, file_utilities):
    
        #super(self.__class__, self).__init__()
        super(UNIX_like_File_Utilities, self).__init__(shell, utilities, session)
    
        self.file_utilities = file_utilities
    
        print("init UNIX_like_File_Utilities.  " + \
            "File Utilities: " + str(file_utilities))
    
    # end of function __init__
    
    def __del__(self):
    
        print("clean UNIX_like_File_Utilities")
    
    # end of function __del__
    
    ##
    #   @brief              List all files, directories, and symbolic links 
    #                       in a directory.
    #
    #   @param  parent_path The path to the directory to be listed.
    #   @param  list_type   The type of entry to be listed.  
    #                       Valid values, "all" and "directory".
    #   @param  launch      Whether to launch the generated command line.
    #
    #   @return             If Param launch is true, 
    #                       content of the Param parent_path in a directory.
    #                       {"status":<T or F>,
    #                       "entries":[<entry 1>,...,<entry N>]}
    #                       If Param launch is false, 
    #                       the generated command line.
    #
    def list(self, parent_path, list_type, launch=True):

        # pre-process parent_path
        #character_to_escape = ['"', '\'', '\\', '/']
        character_to_escape = ['"', '\\']
        parent_path_escaped = \
            self.escapePath(parent_path, character_to_escape)

        command_line = \
            self.file_utilities.list(parent_path_escaped, list_type)
        print("cmd: " + str(command_line))
        
        return_value = {}
        
        if not launch:
            return_value["status"] = True
            return_value["command line"] = command_line
            return return_value

        result = \
            self.launch(command_line)
            
        print("result in UNIX File Util: " + str(result))
        
        if not result["exit status"] == 0:
            # remote error
            # find out where the failure is
            # recover if necessary
            return_value["status"] = False
            return_value["message"] = "list error"
            return return_value
            
        return_value["entries"] = []
        
        # convert session output into JSON
        result_lines = result["stdout"].split('\n')
        for one_line in result_lines:
            if len(one_line) > 0 and \
                one_line[0] in ['d', 'f', 'l']:
                one_path = {}
                line_segments = one_line.split('/')
                one_path["type"] = line_segments[0]
                one_path["path"] = line_segments[1]
                # guarantee that a directory always ends with /
                if one_path["type"] == 'd' and \
                    not one_path["path"]\
                            [len(one_path["path"]) - 1] == '/':
                    one_path["path"] = one_path["path"] + '/'
                one_path["size"] = line_segments[2]
                one_path["time"] = line_segments[3]
                return_value["entries"].append(one_path)
        
        return_value["status"] = True
        
        #print("return_value: " + str(return_value))
        return return_value
        
    # end of function list (implementation of the abstract function)
    
    ##
    #   @brief              Rename a path in the a directory. 
    #
    #   @param  old_path    The full path to be renamed.
    #   @param  new_path    The new path for renaming.
    #                       Only the last segment is used.
    #   @param  launch      Whether to launch the generated command line.
    #
    #   @return             If Param launch is true, 
    #                       Status in a directory.
    #                       {"status":<T or F>}
    #                       If Param launch is false, 
    #                       the generated command line.
    #
    def rename(self, old_path, new_path, launch=True):
        
        #print("old: " + str(old_path) + ",new: " + str(new_path))
        
        old_path_segments = old_path.split('/')
        
        if old_path[len(old_path) - 1] == '/':
            # rename a collection
            old_parent_path = \
                '/'.join(old_path_segments[:len(old_path_segments) - 2]) + '/'
        else:
            old_parent_path = \
                '/'.join(old_path_segments[:len(old_path_segments) - 1]) + '/'
        print("old par: " + str(old_parent_path))
                
        new_path_segments = new_path.split('/')
        
        if new_path[len(new_path) - 1] == '/':
            new_path_last_segment = \
                new_path_segments[len(new_path_segments) - 2]
        else:
            new_path_last_segment = \
                new_path_segments[len(new_path_segments) - 1]
        print("new last: " + str(new_path_last_segment))
        
        # pre-process parent_path
        #character_to_escape = ['"', '\'', '\\', '/']
        character_to_escape = ['"', '\\']
        old_path_escaped = \
            self.escapePath(old_path, character_to_escape)
        old_parent_path_escaped = \
            self.escapePath(old_parent_path, character_to_escape)
        new_path_last_segment_escaped = \
            self.escapePath(new_path_last_segment, character_to_escape)
            
        command_line = \
            self.file_utilities.rename(
                old_path_escaped, 
                old_parent_path_escaped + str(new_path_last_segment_escaped))
        print("cmd ln: " + str(command_line))
        
        return_value = {}
        
        if not launch:
            return_value["status"] = True
            return_value["command line"] = command_line
            return return_value

        result = \
            self.launch(command_line)
        
        if not result["exit status"] == 0:
            # remote error
            # find out where the failure is
            # recover if necessary
            return_value["status"] = False
            return_value["message"] = "rename error"
        
        return_value["status"] = True
        
        return return_value
        
    # end of abstract function rename (implementation of the abstract function)
    
    def remove(self, path, launch=True):
        
        # pre-process parent_path
        #character_to_escape = ['"', '\'', '\\', '/']
        character_to_escape = ['"', '\\']
        path_escaped = \
            self.escapePath(path, character_to_escape)
        
        command_line = \
            self.file_utilities.remove(path_escaped)
        print("cmd ln: " + str(command_line))
        
        return_value = {}
        
        if not launch:
            return_value["status"] = True
            return_value["command line"] = command_line
            return return_value

        result = \
            self.launch(command_line)
        
        if not result["exit status"] == 0:
            # remote error
            # find out where the failure is
            # recover if necessary
            return_value["status"] = False
            return_value["message"] = "remove error"
        
        return_value["status"] = True
        
        return return_value
        
    # end of abstract function remove (implementation of the abstract function)
    
    ##
    #   @brief              Copy a path in the a directory. 
    #
    #   @param  old_path    The full path to be renamed.
    #   @param  new_path    The new path for renaming.
    #                       Only the last segment is used.
    #   @param  launch      Whether to launch the generated command line.
    #
    #   @return             If Param launch is true, 
    #                       Status in a directory.
    #                       {"status":<T or F>}
    #                       If Param launch is false, 
    #                       the generated command line.
    #
    def copy(self, origin_path, copy_path, launch=True):

        # pre-process paths
        #character_to_escape = ['"', '\'', '\\', '/']
        character_to_escape = ['"', '\\']
        util = utility()
        
        origin_path_escaped = \
            util.escapeString(origin_path, character_to_escape)
        #print("esc orig: " + str(origin_path_escaped))

        copy_path_escaped = \
            util.escapeString(copy_path, character_to_escape)
        #print("esc copy: " + str(copy_path_escaped))
            
        command_line = \
            self.file_utilities.copy(
                origin_path_escaped, 
                copy_path_escaped)
        
        #print("cmd ln: " + str(command_line))
        
        return_value = {}
        
        if not launch:
            return_value["status"] = True
            return_value["command line"] = command_line
            return return_value

        result = \
            self.launch(command_line)
        
        if not result["exit status"] == 0:
            # remote error
            # find out where the failure is
            # recover if necessary
            return_value["status"] = False
            return_value["message"] = "copy error"
        else:
            return_value["status"] = True
        
        return return_value
        
    # end of abstract function copy (implementation of the abstract function)
    
    def create(self, path, launch=True):
        
        # pre-process parent_path
        #character_to_escape = ['"', '\'', '\\', '/']
        character_to_escape = ['"', '\\']
        util = utility()
        
        path_escaped = \
            util.escapeString(path, character_to_escape)
        
        command_line = \
            self.file_utilities.create(path_escaped)
            
        print("cmd ln: " + str(command_line))
        
        return_value = {}
        
        if not launch:
            return_value["status"] = True
            return_value["command line"] = command_line
            return return_value

        result = \
            self.launch(command_line)
        
        if not result["exit status"] == 0:
            # remote error
            # find out where the failure is
            # recover if necessary
            return_value["status"] = False
            return_value["message"] = "create error"
        else:
            return_value["status"] = True
        
        return return_value
        
    # end of abstract function create (implementation of the abstract function)
    
    def getSize(self, path, launch=True):
        
        # pre-process parent_path
        #character_to_escape = ['"', '\'', '\\', '/']
        character_to_escape = ['"', '\\']
        util = utility()
        
        path_escaped = \
            util.escapeString(path, character_to_escape)
        
        command_line = \
            self.file_utilities.getSize(path_escaped)
            
        print("cmd ln: " + str(command_line))
        
        return_value = {}
        
        if not launch:
            return_value["status"] = True
            return_value["command line"] = command_line
            return return_value

        result = \
            self.launch(command_line)
        
        if not result["exit status"] == 0:
            # remote error
            # find out where the failure is
            # recover if necessary
            return_value["status"] = False
            return_value["message"] = "getSize error"
        else:
            return_value["status"] = True
        
        return return_value
        
    # end of abstract function getSize (implementation of the abstract function)
    
    def getACL(self, path):
    
        pass
    
    # end of abstract function getACL (implementation of the abstract function)
    
    def setACL(self, path, privilege):
    
        pass
    
    # end of abstract function setACL (implementation of the abstract function)
    
    def modifyACL(self, path, privilege):
    
        pass
    
    # end of abstract function modifyACL
    # (implementation of the abstract function)
    
    def removeACL(self, path, privilege):
    
        pass
    
    # end of abstract function removeACL
    # (implementation of the abstract function)

# end of class File_Operation

if __name__ == "__main__":

    obj = UNIX_like_File_Utilities("BOURNE AGAIN SHELL 4", "GNU UTILITIES 8", "SSH SESSION", "GNU UTILITIES 8")
    
    print("unit test: " + type(obj).__name__)
    
# end of function __main__
