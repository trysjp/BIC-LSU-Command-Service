from __future__ import print_function

import abc

class Authentication(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def generateControlCredential():
    
        pass
    
    # end of abstract function generateControlCredential

# end of class Authentication
