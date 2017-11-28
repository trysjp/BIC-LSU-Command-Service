from __future__ import print_function

import json

from irods.session import iRODSSession
from irods.models import Collection, User, DataObject
from irods.access import iRODSAccess

from File_Operation import *

# API returns empty result while raising CAT_NO_ROWS_FOUND

class iRODS_PythonClient(File_Operation):

    def __init__(self, session):
    
        super(self.__class__, self).__init__()
    
        self.session = session
    
        print("init iRODS_PythonClient")
    
    # end of function __init__
    
    def __del__(self):
    
        print("clean iRODS_PythonClient")
    
    # end of function __del__
    
    ##
    #   @brief              List all objects or collections in a collection.
    #
    #   @param  parent_path The path to the collection to be listed.
    #   @param  list_type   The type of entry to be listed.  
    #                       Valid values, "all" and "directory".
    #
    #   @return             Content of the Param parent_path in a directory.
    #                       {"status":<T or F>,
    #                       "entries":[<entry 1>,...,<entry N>]}
    #
    def list(self, parent_path, list_type="all"):
        
        return_value = {}
        
        if not parent_path[len(parent_path) - 1] == '/':
            return_value["status"] = False
            return_value["message"] = "Only a directory can be listed."
            return json.dumps(return_value)
      
        # path passed to the API must not end with '/'
        parent_path = \
            parent_path[:len(parent_path) - 1]
        
        try:
            collection = \
                self.session.getSession().collections.get(parent_path)
        except Exception as e:
            return_value["status"] = False
            return_value["message"] = e.message
        else:
            return_value["entries"] = []
            for one_collection in collection.subcollections:
                one_path = {}
                one_path["type"] = 'd'
                one_path["path"] = str(one_collection.name) + '/'
                return_value["entries"].append(one_path)
            if list_type == "all":
                for one_object in collection.data_objects:
                    one_path = {}
                    one_path["type"] = 'f'
                    one_path["path"] = str(one_object.name)
                    one_path["size"] = str(one_object.size)
                    one_path["time"] = str(one_object.modify_time)
                    return_value["entries"].append(one_path)
                
            return_value["status"] = True
        
        return json.dumps(return_value)
        
    # end of function list (implementation of the abstract function)
    
    ##
    #   @brief             Rename a path.
    #
    #   @param  old_path   The path to be renamed.
    #   @param  new_path   The new path.  
    #                       
    #   @return            Status in a directory.
    #                      {"status":<T or F>}
    #
    def rename(self, old_path, new_path):
        
        # path passed to the API must not end with '/'
        if new_path[len(new_path) - 1] == '/':
            new_path = \
                new_path[:len(new_path) - 1]
        
        old_path_segments = old_path.split('/')
        new_path_segments = new_path.split('/')
        new_path_last_segment = new_path_segments[len(new_path_segments) - 1]
        
        return_value = {}
        
        try:
            if old_path[len(old_path) - 1] == '/':
                # rename a collection
                #print("rename col")
                old_parent_path = \
                    '/'.join(old_path_segments\
                        [:len(old_path_segments) - 2])
                #print("old: " + 
                #    str(old_path[:len(old_path) - 1]))
                #print("new: " + 
                #    str(old_parent_path + '/' + str(new_path_last_segment)))
                self.session.getSession().collections.move(
                    old_path[:len(old_path) - 1], 
                    old_parent_path + '/' + str(new_path_last_segment))
            else:
                #print("rename obj")
                old_parent_path = \
                    '/' + '/'.join(old_path_segments\
                        [1:len(old_path_segments) - 1])
                #print("old: " + 
                #    str(old_path))
                #print("new: " + 
                #    str(old_parent_path + '/' + str(new_path_last_segment)))
                self.session.getSession().data_objects.move(
                    old_path, 
                    old_parent_path + '/' + str(new_path_last_segment))
        except Exception as e:
            return_value["status"] = False
            return_value["message"] = e.message
        else:
            return_value["status"] = True
        
        return json.dumps(return_value)
        
    # end of abstract function rename (implementation of the abstract function)
    
    ##
    #   @brief          Remove a path.
    #
    #   @param  path    The path to be removed.
    #                       
    #   @return         Status in a directory.
    #                   {"status":<T or F>}
    #
    def remove(self, path):
        
        # path passed to the API must not end with '/'
        if path[len(path) - 1] == '/':
            new_path = path[:len(path) - 1]
        
        return_value = {}
        
        try:
            if path[len(path) - 1] == '/':
                # remove a collection
                self.session.getSession().collections.remove(new_path, 
                    recurse=True, force=True)
            else:
                self.session.getSession().data_objects.unlink(path)
        except Exception as e:
            return_value["status"] = False
            return_value["message"] = e.message
        else:
            return_value["status"] = True
        
        return json.dumps(return_value)
        
    # end of abstract function remove (implementation of the abstract function)
    
    ##
    #   @brief              Copy a path within one iRODS Zone.
    #
    #   @param  origin_path The origin path.
    #   @param  copy_path   The copy destination path.
    #                       
    #   @return             Status in a directory.
    #                       {"status":<T or F>}
    #
    def copy(self, origin_path, copy_path):

        return_value = {}
        return_value["status"] = False
        return_value["message"] = "Operation temporarily not supported."
        
        return json.dumps(return_value)
    
        if origin_path[len(origin_path) - 1] == '/':
            new_origin_path = origin_path[:len(origin_path) - 1]
        if copy_path[len(copy_path) - 1] == '/':
            new_copy_path = copy_path[:len(copy_path) - 1]
    
        return_value = {}
    
        try:
            if origin_path[len(origin_path) - 1] == '/':
                self.session.getSession().collections.copy(
                    new_source_path, new_destination_path)
            else:
                self.session.getSession().data_objects.copy(
                    new_source_path, new_destination_path)
        except Exception as e:
            return_value["status"] = False
            return_value["message"] = e.message
        else:
            return_value["status"] = True
                
        return json.dumps(return_value)
        
    # end of abstract function copy (implementation of the abstract function)


    def move(self, source_path, destination_path):
        
        # path passed to the API must not end with '/'
        if source_path[len(source_path) - 1] == '/':
            source_path = source_path[:len(source_path) - 1]
        if destination_path[len(destination_path) - 1] == '/':
            destination_path = destination_path[:len(destination_path) - 1]
            
        return_value = {}
            
        self.session.getSession.data_objects.move(
            source_path, destination_path)
            
        return_value["status"] = True
        
        return return_value
        
    # end of abstract function move (implementation of the abstract function)
    
    ##
    #   @brief          Create a collection.
    #
    #   @param  path    The path to the new collection.
    #                       
    #   @return         Status in a directory.
    #                   {"status":<T or F>}
    #
    def create(self, path):
        
        # path passed to the API must not end with '/'
        if path[len(path) - 1] == '/':
            path = path[:len(path) - 1]
        
        return_value = {}
        
        try:
            self.session.getSession().collections.create(path)
        except Exception as e:
            return_value["status"] = False
            return_value["message"] = e.message
        else:
            return_value["status"] = True
        
        return json.dumps(return_value)
        
    # end of abstract function create (implementation of the abstract function)
    
    def getACL(self, path):
    
        pass
    
    # end of function getACL
    
    def setACL(self, path, privilege):
    
        pass
    
    # end of function setACL
    
    def modifyACL(self, path, privilege):
    
        pass
    
    # end of function modifyACL
    
    def removeACL(self, path, privilege):
    
        pass
    
    # end of function removeACL

# end of class iRODS_PythonClient

if __name__ == "__main__":

    obj = iRODS_PythonClient()
    
    print("unit test: " + type(obj).__name__)
    
# end of function __main__
