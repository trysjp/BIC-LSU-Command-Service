from __future__ import print_function

from UNIX_like_Shell import *

class POSIX_Shell(UNIX_like_Shell):

    def __init__(self):
    
        #super(self.__class__, self).__init__()
        super(POSIX_Shell, self).__init__()
    
        print("init POSIX_Shell")
    
    # end of function __init__
    
    def __del__(self):
    
        print("clean POSIX_Shell")
    
    # end of function __del__
    
    def list(self, path, list_type="all", return_type="result"):
      
        command_line = ''
        command_line += 'parent_path="' + path + '" && echo "BIC-LSU 01" && '
        command_line += 'child_paths=$(find "${parent_path}" -maxdepth 1 '
        if list_type == "directory":
            command_line += '-type d '
        elif not list_type == "all":
            return None
        command_line += '-printf "%y/%P/%s/%a\\n" | tail --lines +2) && echo "BIC-LSU 02" && '
        command_line += 'echo "${child_paths}" | awk -F"/" "{print \$1\\"/\\"\$2\\"/\\"\$3\\"/\\"substr(\$4,1,19)}" && echo "BIC-LSU 03 && "'
        command_line += 'echo "BIC-LSU succeeded"'
        
        if return_type == "result":
            return self.launch("SSH SESSION", "COMMAND")
        elif return_type == "command":
            return command_line
        else:
            return None
        
    # end of function list (implementation of the abstract method)
    
    def rename(self, old_path, new_path_last_segment, return_type = "result"):
        
        command_line = ''
        command_line += 'old_path="' + old_path + '" && echo "BIC-LSU 01" && '
        command_line += 'new_path="' + new_path_last_segment + '" && echo "BIC-LSU 02" && '
        command_line += 'mv --no-clobber "${old_path}" "${new_path}" && echo "BIC-LSU 03" && '
        command_line += 'echo "BIC-LSU succeeded"'
        
        if return_type == "result":
            return self.launch("SSH SESSION", "COMMAND")
        elif return_type == "command":
            return command_line
        else:
            return None
        
    # end of abstract function rename (implementation of the abstract method)
    
    def remove(self, path):
        pass
    # end of abstract function remove (implementation of the abstract method)
    
    def copy(self, origin_path, copy_path):
        pass
    # end of abstract function copy (implementation of the abstract method)
    
    def move(self, source_path, destination_path):
        pass
    # end of abstract function move (implementation of the abstract method)
    
    def make_directory(self, path):
        pass
    # end of abstract function make_directory
    # (implementation of the abstract method)

# end of class POSIX_Shell

if __name__ == "__main__":

    obj = POSIX_Shell()
    
    print("unit test: " + type(obj).__name__)
    
    return_value = \
        obj.list("test/path", list_type = "all", return_type = "command")
    print(str(return_value))
    
# end of function __main__
