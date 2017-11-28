from __future__ import print_function

import paramiko

from SSH_Session import *
from File_Operation import *

class UNIX_like_File_Interface(File_Operation):

    def __init__(self, shell, utilities, session):
    
        #super(self.__class__, self).__init__()
        super(UNIX_like_File_Interface, self).__init__()
        
        self.shell = shell
        self.utilities = utilities
        self.session = session
    
        print("init UNIX_like_File_Interface.  " + 
            "Shell: " + str(self.shell) + ".  " + 
            "Utilities: " + str(self.utilities) + ".  " + 
            "Session: " + str(self.session))
    
    # end of function __init__
    
    def __del__(self):
    
        print("clean UNIX_like_File_Interface")
    
    # end of function __del__
    
    ##
    #   @brief              Escape characters in a path string.
    #
    #   @param  path        The path to be escaped.
    #   @param  characters  A list of characters to be escaped.
    #
    #   @return             A string with all characters 
    #                       in the Parameter characters escaped.
    #
    def escapePath(self, path, characters):
    
        escaped_path = ""
        for index in range(0, len(path), 1):
            if path[index] in characters:
                escaped_path += '\\' + path[index]
            else:
                escaped_path += path[index] 
            
        return escaped_path
    
    # end of function escapePath
    
    ##
    #   @brief              Launch a command line via the established session.
    #
    #   @param  path        The path to be escaped.
    #   @param  characters  A list of characters to be escaped.
    #
    #   @return             A string with all characters 
    #                       in the Parameter characters escaped.
    #
    def launch(self, command_line): 
        
        print("run Command " + command_line + " via Session " + str(self.session))
        
        return self.session.executeCommandLine(command_line)
        
    # end of function launch

# end of class UNIX_like_File_Interface

if __name__ == "__main__":

    obj = UNIX_like_File_Interface()
    
    print("unit test: " + type(obj).__name__)
    
# end of function __main__
