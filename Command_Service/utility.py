from __future__ import print_function

class utility(object):

    def __init__(self):
    
        super(utility, self).__init__()
    
        print("init utility")
    
    # end of function __init__
    
    def __del__(self):
    
        print("clean utility")
    
    # end of function __del__
    
    ##
    #   @brief              Escapes characters in a string.
    #
    #   @param  string      The string to be escaped.
    #   @param  characters  A list of characters to be escaped.
    #
    #   @return             A string with all characters 
    #                       in the Parameter characters escaped.
    #
    def escapeString(self, string, characters):
    
        escaped_string = ""
        
        for index in range(0, len(string), 1):
        
            if string[index] in characters:
                escaped_string += '\\'
            
            escaped_string += string[index] 
            
        return escaped_string
    
    # end of function escapeString
    
    ##
    #   @brief                  Quotes a command line .
    #
    #   @param  command_line    The command line to be quoted.
    #
    #   @return                 A quoted command line.
    #
    def quoteCommandLine(self, command_line):
    
        quoted_command_line = "'"
        
        for index in range(0, len(command_line), 1):
        
            if command_line[index] == '\'':
                quoted_command_line += "'\\''"
            else:
                quoted_command_line += command_line[index] 
            
        quoted_command_line += "'"
            
        return quoted_command_line
    
    # end of function quoteCommandLine

# end of class utility

if __name__ == "__main__":

    command_line = "hello 'world', jack"
    
    util = utility()
    
    print("quoted: " + str(util.quoteCommandLine(command_line)))

# end of __main__
