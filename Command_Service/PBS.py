from __future__ import print_function

from UNIX_like_Job_Control import *

class PBS(UNIX_like_Job_Control):

    def __init__(self, shell, utilities, session, parsing_information):
    
        #super(self.__class__, self).__init__(shell, utilities, session)
        super(PBS, self).__init__(
            shell, utilities, session, parsing_information)
    
        print("init PBS")
    
    # end of function __init__
    
    def __del__(self):
    
        print("clean PBS")
    
    # end of function __del__
    
    def submitJob():
        pass
    # end of function submitJob (implementation of the abstract function)
    
    def deleteJob():
        pass
    # end of function deleteJob (implementation of the abstract function)
    
    def showJobStatus():
        pass
    # end of function showJobStatus
    # (implementation of the abstract function)
    
    def estimateJobStartingTime():
        pass
    # end of abstract estimateJobStartingTime
    # (implementation of the abstract function)

# end of class PBS

if __name__ == "__main__":

    obj = PBS("KORN SHELL", "GNU UTILITIES 8", "SSH SESSION")
    
    print("unit test: " + type(obj).__name__)
    
# end of function __main__
