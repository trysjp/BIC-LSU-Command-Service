from __future__ import print_function

from Job_Control import *

class Oozie_REST(Job_Control):

    def __init__(self):
    
        print("init Oozie_REST")
    
    # end of function __init__

# end of class Oozie_REST

if __name__ == "__main__":

    obj = Oozie_REST()
    
    print("unit test: " + type(obj).__name__)
    
# end of function __main__
