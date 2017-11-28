from __future__ import print_function

import sqlite3
import time
import traceback

from DB_Operator import *
from DB_Initializer import *

class BICLSU_DB_Operator(object):

    def __init__(self, BICLSU_db_path):
        
        self.db_operator = SQLite_DB_Operator(BICLSU_db_path)
    
    # end of function __init__
    
    def createDB(self):
    
        # Create table entity__BICLSU_User

        query_string = """
        
            CREATE TABLE 
                entity__BICLSU_User 
                (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT, 
                    display_name TEXT NOT NULL, 
                    myLSU_Id TEXT NOT NULL UNIQUE, 
                    Elgg_user_Id TEXT NOT NULL UNIQUE, 
                    iRODS_user_Id TEXT NOT NULL UNIQUE,
                    iRODS_password TEXT NOT NULL,
                    human_admin INTEGER NOT NULL
                );
        
            CREATE TABLE 
                entity__BICLSU_Group 
                (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    display_name TEXT NOT NULL,
                    Elgg_group_Id INTEGER NOT NULL UNIQUE,
                    iRODS_group_Id TEXT NOT NULL UNIQUE
                );
                
            CREATE TABLE 
                entity__iRODS_Resource 
                (
                    Id TEXT NOT NULL UNIQUE
                );
                
            CREATE TABLE 
                entity__Host 
                (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT, 
                    display_name TEXT NOT NULL
                );
                
            CREATE TABLE 
                entity__Affiliate 
                (
                    Id TEXT PRIMARY KEY
                );
                
            CREATE TABLE 
                entity_weak__Private_User 
                (
                    host_Id, 
                    Id INTEGER NOT NULL UNIQUE, 
                    private_user_account TEXT NOT NULL,
                    FOREIGN KEY (
                        host_Id)
                        REFERENCES 
                            entity__Host(
                                Id) 
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    PRIMARY KEY (
                        host_Id, 
                        Id),
                    UNIQUE (
                        host_Id,
                        private_user_account)
                );
                
            CREATE TABLE
                entity__Control_Credential
                (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    category TEXT NOT NULL,
                    path TEXT NOT NULL UNIQUE,
                    passphrase TEXT NOT NULL
                );
                
            CREATE TABLE
                entity_weak__Passphrase
                (
                    control_credential_Id,
                    Id INTEGER NOT NULL UNIQUE,
                    passphrase TEXT NOT NULL,
                    FOREIGN KEY (
                        control_credential_Id)
                        REFERENCES
                            entity__Control_Credential(
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    PRIMARY KEY (
                        control_credential_Id,
                        Id)
                );
                
            CREATE TABLE 
                entity_weak__Private_Resource 
                (
                    host_Id, 
                    private_user_Id, 
                    Id INTEGER NOT NULL UNIQUE, 
                    display_name TEXT NOT NULL, 
                    root_absolute_path TEXT NOT NULL,
                    allow_external_acl INTEGER NOT NULL,
                    FOREIGN KEY (
                        host_Id) 
                        REFERENCES 
                            entity__Host(
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    FOREIGN KEY (
                        private_user_Id) 
                        REFERENCES 
                            entity_weak__Private_User(
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    PRIMARY KEY (
                        host_Id, 
                        private_user_Id, 
                        Id)
                );
                
            CREATE TABLE 
                entity__Service 
                ( 
                    Id INTEGER PRIMARY KEY AUTOINCREMENT, 
                    category TEXT NOT NULL, 
                    server_type TEXT NOT NULL,
                    server_version TEXT NOT NULL,
                    display_name TEXT NOT NULL
                );
                
            CREATE TABLE 
                entity_weak__Network_Interface 
                (
                    host_Id, 
                    Id NOT NULL UNIQUE, 
                    capacity INTEGER NOT NULL, 
                    IPv4_or_FQDN TEXT NOT NULL, 
                    IPv6_or_FQDN TEXT NOT NULL,
                    network_type TEXT NOT NULL,
                    FOREIGN KEY (
                        host_Id) 
                        REFERENCES 
                            entity__Host(
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    PRIMARY KEY (
                        host_Id, 
                        Id)
                );
                
            CREATE TABLE 
                entity_weak__Control_Port 
                (
                    host_Id, 
                    number INTEGER NOT NULL, 
                    FOREIGN KEY (host_Id) 
                        REFERENCES 
                            entity__Host(Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    PRIMARY KEY (
                        host_Id, 
                        number)
                );
                
            CREATE TABLE 
                entity_weak__Data_Port
                (
                    host_Id, 
                    number INTEGER NOT NULL,
                    in_use INTEGER NOT NULL, 
                    FOREIGN KEY (host_Id) 
                        REFERENCES 
                            entity__Host(Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    PRIMARY KEY (
                        host_Id, 
                        number)
                );
                
            CREATE TABLE 
                entity_weak__Directory_Annotation
                (
                    host_Id, 
                    private_user_Id, 
                    private_resource_Id,
                    Id INTEGER NOT NULL UNIQUE,
                    relative_path TEXT NOT NULL,
                    sync_time INTEGER NOT NULL, 
                    FOREIGN KEY (
                        host_Id, 
                        private_user_Id, 
                        private_resource_Id) 
                        REFERENCES 
                            entity_weak__Private_Resource(
                                host_Id, 
                                private_user_Id, 
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    PRIMARY KEY (
                        host_Id, 
                        private_user_Id, 
                        private_resource_Id, 
                        Id),
                    UNIQUE (
                        host_Id, 
                        private_user_Id, 
                        private_resource_Id,
                        relative_path)
                        ON CONFLICT ABORT
                );
                
            CREATE TABLE 
                entity_weak__Annotation 
                (
                    host_Id, 
                    private_user_Id, 
                    private_resource_Id,
                    directory_annotation_Id,
                    Id INTEGER NOT NULL UNIQUE,
                    annotation TEXT NOT NULL,
                    FOREIGN KEY (
                        host_Id, 
                        private_user_Id, 
                        private_resource_Id, 
                        directory_annotation_Id) 
                        REFERENCES 
                            entity_weak__Directory_Annotation(
                                host_Id,
                                private_user_Id,
                                private_resource_Id,
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    PRIMARY KEY (
                        host_Id, 
                        private_user_Id, 
                        private_resource_Id, 
                        directory_annotation_Id, 
                        Id)
                );
                
            CREATE TABLE 
                entity_weak__Directory_ACL 
                (
                    host_Id, 
                    private_user_Id, 
                    private_resource_Id,
                    Id INTEGER NOT NULL UNIQUE,
                    relative_path TEXT NOT NULL,
                    sync_time INTEGER NOT NULL,
                    FOREIGN KEY (
                        host_Id, 
                        private_user_Id, 
                        private_resource_Id) 
                        REFERENCES 
                            entity_weak__Private_Resource(
                                host_Id, 
                                private_user_Id, 
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    PRIMARY KEY (
                        host_Id, 
                        private_user_Id, 
                        private_resource_Id, 
                        Id),
                    UNIQUE (
                        host_Id, 
                        private_user_Id, 
                        private_resource_Id,
                        relative_path)
                        ON CONFLICT ABORT
                );
                
            CREATE TABLE 
                entity__Transfer_Application 
                (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    display_name TEXT NOT NULL,
                    priority INTEGER NOT NULL
                );
                
            CREATE TABLE 
                entity__Analytic_Application 
                (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    display_name TEXT NOT NULL UNIQUE
                );
                
            CREATE TABLE 
                relationship__BICLSU_User__is_a_friend_of__BICLSU_User 
                (
                    biclsu_user_Id_small, 
                    biclsu_user_Id_large,
                    FOREIGN KEY (
                        biclsu_user_Id_small) 
                        REFERENCES 
                            entity__BICLSU_User(
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    FOREIGN KEY (
                        biclsu_user_Id_large) 
                        REFERENCES 
                            entity__BICLSU_User(
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    PRIMARY KEY (
                        biclsu_user_Id_small, 
                        biclsu_user_Id_large),
                    CHECK (
                        biclsu_user_Id_small < biclsu_user_Id_large) 
                        ON CONFLICT ROLLBACK
                );
                
            CREATE TABLE 
                relationship__BICLSU_Group__consists_of__BICLSU_User 
                (
                    biclsu_group_Id, 
                    biclsu_user_Id,
                    is_admin INTEGER NOT NULL,
                    FOREIGN KEY (
                        biclsu_group_Id) 
                        REFERENCES 
                            entity__BICLSU_Group(
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    FOREIGN KEY (
                        biclsu_user_Id) 
                        REFERENCES 
                            entity__BICLSU_User(
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    PRIMARY KEY (
                        biclsu_group_Id, 
                        biclsu_user_Id)
                );
                
            CREATE TABLE 
                relationship__BICLSU_User__maps_to__Private_User 
                (
                    biclsu_user_Id, 
                    private_user_host_Id, 
                    private_user_Id,
                    FOREIGN KEY (
                        biclsu_user_Id) 
                        REFERENCES 
                            entity__BICLSU_User(
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    FOREIGN KEY (
                        private_user_host_Id,
                        private_user_Id) 
                        REFERENCES 
                            entity_weak__Private_User(
                                host_Id,
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    PRIMARY KEY (
                        biclsu_user_Id, 
                        private_user_host_Id, 
                        private_user_Id)
                );
                
            CREATE TABLE
                relationship__Private_User__accepts__Control_Credential
                (
                    private_user_host_Id,
                    private_user_Id,
                    control_credential_Id,
                    FOREIGN KEY (
                        private_user_host_Id,
                        private_user_Id)
                        REFERENCES
                            entity_weak__Private_User(
                                host_Id,
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    FOREIGN KEY (
                        control_credential_Id)
                        REFERENCES
                            entity__Control_Credential(
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    PRIMARY KEY (
                        private_user_host_Id,
                        private_user_Id,
                        control_credential_Id)
                );
                
            CREATE TABLE
                relationship_weak__Control_Credential__protected_by__Passphrase
                (
                    control_credential_Id,
                    passphrase_control_credential_Id,
                    passphrase_Id,
                    FOREIGN KEY (
                        control_credential_Id)
                        REFERENCES
                            entity__Control_Credential(
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    FOREIGN KEY (
                        passphrase_control_credential_Id,
                        passphrase_Id)
                        REFERENCES
                            entity_weak__Passphrase(
                                control_credential_Id,
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    PRIMARY KEY (
                        control_credential_Id,
                        passphrase_control_credential_Id,
                        passphrase_Id)
                );
                
            CREATE TABLE 
                relationship__BICLSU_User__frequently_accesses__Private_Resource 
                (
                    biclsu_user_Id, 
                    private_resource_host_Id, 
                    private_resource_private_user_Id,
                    private_resource_Id,
                    FOREIGN KEY (
                        biclsu_user_Id) 
                        REFERENCES 
                            entity__BICLSU_User(
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    FOREIGN KEY (
                        private_resource_host_Id,
                        private_resource_private_user_Id,
                        private_resource_Id) 
                        REFERENCES 
                            entity_weak__Private_Resource(
                                host_Id,
                                private_user_Id,
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    PRIMARY KEY (
                        biclsu_user_Id, 
                        private_resource_host_Id, 
                        private_resource_private_user_Id,
                        private_resource_Id)
                );
                
            CREATE TABLE 
                relationship_weak__Private_User__resides_on__Host 
                (
                    private_user_host_Id,
                    private_user_Id, 
                    host_Id, 
                    FOREIGN KEY (
                        private_user_host_Id, 
                        private_user_Id) 
                        REFERENCES 
                            entity_weak__Private_User(
                                host_Id, 
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    FOREIGN KEY (
                        host_Id) 
                        REFERENCES 
                            entity__Host(
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    PRIMARY KEY (
                        private_user_host_Id, 
                        private_user_Id, 
                        host_Id)
                );
                
            CREATE TABLE 
                relationship__BICLSU_User__prefers__iRODS_Resource 
                (
                    biclsu_user_Id, 
                    iRODS_resource_Id,
                    FOREIGN KEY (
                        biclsu_user_Id) 
                        REFERENCES 
                            entity__BICLSU_User(
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    FOREIGN KEY (
                        iRODS_resource_Id) 
                        REFERENCES 
                            entity__iRODS_Resource(
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    PRIMARY KEY (
                        biclsu_user_Id, 
                        iRODS_resource_Id)
                );
                
            CREATE TABLE 
                relationship_weak__Private_User__has__Private_Resource 
                (
                    private_user_host_Id,
                    private_user_Id,
                    private_resource_host_Id,  
                    private_resource_private_user_Id,
                    private_resource_Id,
                    FOREIGN KEY (
                        private_user_host_Id, 
                        private_user_Id) 
                        REFERENCES 
                            entity_weak__Private_User(
                                host_Id,
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    FOREIGN KEY (
                        private_resource_host_Id, 
                        private_resource_private_user_Id, 
                        private_resource_Id) 
                        REFERENCES 
                            entity_weak__Private_Resource(
                                host_Id, 
                                private_user_Id, 
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    PRIMARY KEY (
                        private_user_host_Id, 
                        private_user_Id, 
                        private_resource_host_Id, 
                        private_resource_private_user_Id, 
                        private_resource_Id)
                );
                
            CREATE TABLE 
                relationship_weak__Private_Resource__resides_on__Host 
                (
                    private_resource_host_Id,
                    private_resource_private_user_Id, 
                    private_resource_Id,
                    host_Id, 
                    FOREIGN KEY (
                        private_resource_host_Id, 
                        private_resource_private_user_Id, 
                        private_resource_Id) 
                        REFERENCES 
                            entity_weak__Private_Resource(
                                host_Id, 
                                private_user_Id, 
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    FOREIGN KEY (
                        host_Id) 
                        REFERENCES 
                            entity__Host(
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    PRIMARY KEY (
                        private_resource_host_Id, 
                        private_resource_private_user_Id, 
                        private_resource_Id, 
                        host_Id)
                );
                
            CREATE TABLE 
                relationship__iRODS_Resource__resides_on__Host 
                (
                    iRODS_resource_Id,
                    host_Id,  
                    FOREIGN KEY (
                        iRODS_resource_Id) 
                        REFERENCES 
                            entity__iRODS_Resource(
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    FOREIGN KEY (
                        host_Id) 
                        REFERENCES 
                            entity__Host(
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    PRIMARY KEY (
                        iRODS_resource_Id, 
                        host_Id)
                );
                
            CREATE TABLE 
                relationship__Private_Resource__holds_credential__Private_Resource 
                (
                    private_resource_with_credential_host_Id,
                    private_resource_with_credential_private_user_Id, 
                    private_resource_with_credential_Id,
                    private_resource_host_Id,
                    private_resource_private_user_Id, 
                    private_resource_Id, 
                    authentication_category TEXT NOT NULL,
                    identity_relative_path TEXT NOT NULL,
                    forwardable INTEGER NOT NULL,
                    FOREIGN KEY (
                        private_resource_with_credential_host_Id,
                        private_resource_with_credential_private_user_Id, 
                        private_resource_with_credential_Id) 
                        REFERENCES 
                            entity_weak__Private_Resource(
                                host_Id, 
                                private_user_Id, 
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    FOREIGN KEY (
                        private_resource_host_Id,
                        private_resource_private_user_Id, 
                        private_resource_Id) 
                        REFERENCES 
                            entity_weak__Private_Resource(
                                host_Id, 
                                private_user_Id, 
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    PRIMARY KEY (
                        private_resource_with_credential_host_Id,
                        private_resource_with_credential_private_user_Id, 
                        private_resource_with_credential_Id,
                        private_resource_host_Id,
                        private_resource_private_user_Id, 
                        private_resource_Id)
                );
                
            CREATE TABLE 
                relationship_weak__Private_Resource__has__Directory_Annotation 
                (
                    private_resource_host_Id,
                    private_resource_private_user_Id, 
                    private_resource_Id,
                    directory_annotation_host_Id, 
                    directory_annotation_private_user_Id,
                    directory_annotation_private_resource_Id,
                    directory_annotation_Id,
                    FOREIGN KEY (
                        private_resource_host_Id,
                        private_resource_private_user_Id,
                        private_resource_Id)
                        REFERENCES 
                            entity_weak__Private_Resource(
                                host_Id,
                                private_user_Id,
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    FOREIGN KEY (
                        directory_annotation_host_Id,
                        directory_annotation_private_user_Id,
                        directory_annotation_private_resource_Id,
                        directory_annotation_Id)
                        REFERENCES 
                            entity_weak__Directory_Annotation(
                                host_Id,
                                private_user_Id,
                                private_resource_Id,
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    PRIMARY KEY (
                        private_resource_host_Id,
                        private_resource_private_user_Id, 
                        private_resource_Id,
                        directory_annotation_host_Id, 
                        directory_annotation_private_user_Id,
                        directory_annotation_private_resource_Id,
                        directory_annotation_Id)
                );
                
            CREATE TABLE 
                relationship_weak__Directory_Annotation__has__Annotation 
                (
                    directory_annotation_host_Id,
                    directory_annotation_private_user_Id, 
                    directory_annotation_private_resource_Id,
                    directory_annotation_Id,
                    annotation_host_Id,
                    annotation_private_user_Id, 
                    annotation_private_resource_Id,
                    annotation_directory_annotation_Id,
                    annotation_Id,
                    FOREIGN KEY (
                        directory_annotation_host_Id,
                        directory_annotation_private_user_Id, 
                        directory_annotation_private_resource_Id,
                        directory_annotation_Id) 
                        REFERENCES 
                            entity_weak__Directory_Annotation(
                                host_Id,
                                private_user_Id,
                                private_resource_Id,
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    FOREIGN KEY (
                        annotation_host_Id,
                        annotation_private_user_Id, 
                        annotation_private_resource_Id,
                        annotation_directory_annotation_Id,
                        annotation_Id) 
                        REFERENCES 
                            entity_weak__Annotation(
                                host_Id,
                                private_user_Id,
                                private_resource_Id,
                                directory_annotation_Id,
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    PRIMARY KEY (
                        directory_annotation_host_Id,
                        directory_annotation_private_user_Id, 
                        directory_annotation_private_resource_Id,
                        directory_annotation_Id,
                        annotation_host_Id,
                        annotation_private_user_Id, 
                        annotation_private_resource_Id,
                        annotation_directory_annotation_Id,
                        annotation_Id)
                );
                
            CREATE TABLE 
                relationship_weak__BICLSU_User_annotates_with__Annotation 
                (
                    biclsu_user_Id,
                    annotation_host_Id,
                    annotation_private_user_Id, 
                    annotation_private_resource_Id,
                    annotation_directory_annotation_Id,
                    annotation_Id,
                    FOREIGN KEY (
                        biclsu_user_Id) 
                        REFERENCES 
                            entity__BICLSU_User(
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    FOREIGN KEY (
                        annotation_host_Id,
                        annotation_private_user_Id, 
                        annotation_private_resource_Id,
                        annotation_directory_annotation_Id,
                        annotation_Id) 
                        REFERENCES 
                            entity_weak__Annotation(
                                host_Id,
                                private_user_Id,
                                private_resource_Id,
                                directory_annotation_Id,
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    PRIMARY KEY (
                        biclsu_user_Id,
                        annotation_host_Id,
                        annotation_private_user_Id, 
                        annotation_private_resource_Id,
                        annotation_directory_annotation_Id,
                        annotation_Id)
                );
                
            CREATE TABLE 
                relationship_weak__Private_Resource__has__Directory_ACL 
                (
                    private_resource_host_Id,
                    private_resource_private_user_Id, 
                    private_resource_Id,
                    directory_ACL_host_Id, 
                    directory_ACL_private_user_Id,
                    directory_ACL_private_resource_Id,
                    directory_ACL_Id,
                    FOREIGN KEY (
                        private_resource_host_Id,
                        private_resource_private_user_Id, 
                        private_resource_Id) 
                        REFERENCES 
                            entity_weak__Private_Resource(
                                host_Id,
                                private_user_Id,
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE, 
                    FOREIGN KEY (
                        directory_ACL_host_Id, 
                        directory_ACL_private_user_Id,
                        directory_ACL_private_resource_Id,
                        directory_ACL_Id) 
                        REFERENCES 
                            entity_weak__Directory_ACL(
                                host_Id,
                                private_user_Id,
                                private_resource_Id,
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,              
                    PRIMARY KEY (
                        private_resource_host_Id,
                        private_resource_private_user_Id, 
                        private_resource_Id,
                        directory_ACL_host_Id, 
                        directory_ACL_private_user_Id,
                        directory_ACL_private_resource_Id,
                        directory_ACL_Id)
                );
                
            CREATE TABLE 
                relationship__BICLSU_User__restricted_by_ACL__Directory_ACL 
                (
                    biclsu_user_Id,
                    IO_privilege INTEGER NOT NULL,
                    computing_privilege INTEGER NOT NULL,
                    recursive INTEGER NOT NULL, 
                    directory_ACL_host_Id, 
                    directory_ACL_private_user_Id,
                    directory_ACL_private_resource_Id,
                    directory_ACL_Id,
                    FOREIGN KEY (
                        biclsu_user_Id)
                        REFERENCES
                            entity__BICLSU_User(
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    FOREIGN KEY (
                        directory_ACL_host_Id,
                        directory_ACL_private_user_Id,
                        directory_ACL_private_resource_Id,
                        directory_ACL_Id) 
                        REFERENCES 
                            entity_weak__Directory_ACL(
                                host_Id,
                                private_user_Id,
                                private_resource_Id,
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    PRIMARY KEY (
                        biclsu_user_Id, 
                        directory_ACL_host_Id, 
                        directory_ACL_private_user_Id, 
                        directory_ACL_private_resource_Id, 
                        directory_ACL_Id)
                );
                
            CREATE TABLE 
                relationship__BICLSU_Group__restricted_by_ACL__Directory_ACL 
                (
                    biclsu_group_Id,
                    IO_privilege INTEGER NOT NULL,
                    computing_privilege INTEGER NOT NULL, 
                    recursive INTEGER NOT NULL,
                    directory_ACL_host_Id, 
                    directory_ACL_private_user_Id,
                    directory_ACL_private_resource_Id,
                    directory_ACL_Id,
                    FOREIGN KEY (
                        biclsu_group_Id)
                        REFERENCES
                            entity__BICLSU_Group(
                            Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    FOREIGN KEY (
                        directory_ACL_host_Id,
                        directory_ACL_private_user_Id,
                        directory_ACL_private_resource_Id,
                        directory_ACL_Id) 
                        REFERENCES 
                            entity_weak__Directory_ACL(
                                host_Id,
                                private_user_Id,
                                private_resource_Id,
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    PRIMARY KEY (
                        biclsu_group_Id, 
                        directory_ACL_host_Id, 
                        directory_ACL_private_user_Id, 
                        directory_ACL_private_resource_Id, 
                        directory_ACL_Id)
                );
                
            CREATE TABLE 
                relationship__BICLSU_User__restricted_by_ACL__Annotation 
                (
                    biclsu_user_Id,
                    privilege INTEGER NOT NULL, 
                    annotation_host_Id, 
                    annotation_private_user_Id,
                    annotation_private_resource_Id,
                    annotation_directory_annotation_Id,
                    annotation_Id,
                    FOREIGN KEY (
                        biclsu_user_Id)
                        REFERENCES 
                            entity__BICLSU_User(
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    FOREIGN KEY (
                        annotation_host_Id, 
                        annotation_private_user_Id,
                        annotation_private_resource_Id,
                        annotation_directory_annotation_Id,
                        annotation_Id) 
                        REFERENCES 
                            entity_weak__Annotation(
                                host_Id,
                                private_user_Id,
                                private_resource_Id,
                                directory_annotation_Id,
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    PRIMARY KEY ( 
                        biclsu_user_Id, 
                        annotation_host_Id, 
                        annotation_private_user_Id, 
                        annotation_private_resource_Id, 
                        annotation_directory_annotation_Id, 
                        annotation_Id)
                );
                
            CREATE TABLE 
                relationship__BICLSU_Group__restricted_by_ACL__Annotation 
                (
                    biclsu_group_Id,
                    privilege INTEGER NOT NULL,
                    annotation_host_Id, 
                    annotation_private_user_Id,
                    annotation_private_resource_Id,
                    annotation_directory_annotation_Id,
                    annotation_Id,
                    FOREIGN KEY (
                        biclsu_group_Id)
                        REFERENCES 
                            entity__BICLSU_Group(
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    FOREIGN KEY (
                        annotation_host_Id, 
                        annotation_private_user_Id,
                        annotation_private_resource_Id,
                        annotation_directory_annotation_Id,
                        annotation_Id) 
                        REFERENCES 
                            entity_weak__Annotation(
                                host_Id,
                                private_user_Id,
                                private_resource_Id,
                                directory_annotation_Id,
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    PRIMARY KEY ( 
                        biclsu_group_Id, 
                        annotation_host_Id, 
                        annotation_private_user_Id, 
                        annotation_private_resource_Id, 
                        annotation_directory_annotation_Id, 
                        annotation_Id)
                );
                
            CREATE TABLE 
                relationship__Private_Resource__supports__Transfer_Application 
                (
                    private_resource_host_Id, 
                    private_resource_private_user_Id,
                    private_resource_Id,
                    transfer_application_Id,
                    transfer_application_absolute_path TEXT NOT NULL,
                    optimal_read_parallelism INTEGER NOT NULL,
                    optimal_read_other_parameters TEXT NOT NULL,
                    optimal_write_parallelism INTEGER NOT NULL,
                    optimal_write_other_parameters TEXT NOT NULL,
                    accept_connection INTEGER NOT NULL,
                    sync_time INTEGER NOT NULL,
                    FOREIGN KEY (
                        private_resource_host_Id,
                        private_resource_private_user_Id,
                        private_resource_Id) 
                        REFERENCES 
                            entity_weak__Private_Resource(
                                host_Id,
                                private_user_Id,
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    FOREIGN KEY (
                        transfer_application_Id) 
                        REFERENCES 
                            entity__Transfer_Application(
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    PRIMARY KEY (
                        private_resource_host_Id, 
                        private_resource_private_user_Id, 
                        private_resource_Id, 
                        transfer_application_Id)
                );
                
            CREATE TABLE 
                relationship_weak__Host__has__Network_Interface 
                (
                    host_Id, 
                    network_interface_host_Id,
                    network_interface_Id,
                    FOREIGN KEY (
                        host_Id) 
                        REFERENCES 
                            entity__Host(
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    FOREIGN KEY (
                        network_interface_host_Id,
                        network_interface_Id) 
                        REFERENCES 
                            entity_weak__Network_Interface(
                                host_Id,
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    PRIMARY KEY (
                        host_Id, 
                        network_interface_host_Id, 
                        network_interface_Id)
                );
                
            CREATE TABLE 
                relationship_weak__Host__has__Control_Port 
                (
                    host_Id, 
                    control_port_host_Id,
                    control_port_number,
                    FOREIGN KEY (
                        host_Id) 
                        REFERENCES 
                            entity__Host(
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    FOREIGN KEY (
                        control_port_host_Id,
                        control_port_number) 
                        REFERENCES 
                            entity_weak__Control_Port(
                                host_Id,
                                number)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    PRIMARY KEY (
                        host_Id, 
                        control_port_host_Id, 
                        control_port_number)
                );
                
            CREATE TABLE 
                relationship_weak__Host__has__Data_Port 
                (
                    host_Id, 
                    data_port_host_Id,
                    data_port_number,
                    FOREIGN KEY (
                        host_Id) 
                        REFERENCES 
                            entity__Host(Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    FOREIGN KEY (
                        data_port_host_Id,
                        data_port_number) 
                        REFERENCES 
                            entity_weak__Data_Port(
                                host_Id,
                                number)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    PRIMARY KEY (
                        host_Id, 
                        data_port_host_Id, 
                        data_port_number)
                );
                
            CREATE TABLE
                gerund__Host__provides__Service
                (
                    host_Id,
                    service_Id,
                    allow_external_acl INTEGER NOT NULL,
                    FOREIGN KEY (
                        host_Id)
                        REFERENCES 
                            entity__Host(
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    FOREIGN KEY (
                        service_Id)
                        REFERENCES 
                            entity__Service(
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    PRIMARY KEY (
                        host_Id,  
                        service_Id)
                );
                
            CREATE TABLE 
                relationship__Host__provides__Service__listens_at__Network_Interface 
                (
                    host_provides_service_host_Id, 
                    host_provides_service_service_Id,
                    network_interface_host_Id,
                    network_interface_Id,
                    FOREIGN KEY (
                        host_provides_service_host_Id,
                        host_provides_service_service_Id) 
                        REFERENCES 
                            gerund__Host__provides__Service(
                                host_Id,
                                service_Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    FOREIGN KEY (
                        network_interface_host_Id,
                        network_interface_Id) 
                        REFERENCES 
                        entity_weak__Network_Interface(
                            host_Id,
                            Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    PRIMARY KEY (
                        host_provides_service_host_Id, 
                        host_provides_service_service_Id, 
                        network_interface_host_Id, 
                        network_interface_Id)
                );
                
            CREATE TABLE 
                relationship__Host__provides__Service__listens_at__Control_Port 
                (
                    host_provides_service_host_Id, 
                    host_provides_service_service_Id,
                    control_port_host_Id,
                    control_port_number,
                    FOREIGN KEY (
                        host_provides_service_host_Id,
                        host_provides_service_service_Id) 
                        REFERENCES 
                            gerund__Host__provides__Service(
                                host_Id,
                                service_Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    FOREIGN KEY (
                        control_port_host_Id,
                        control_port_number) 
                        REFERENCES 
                            entity_weak__Control_Port(
                                host_Id,
                                number)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    PRIMARY KEY (
                        host_provides_service_host_Id, 
                        host_provides_service_service_Id, 
                        control_port_host_Id, 
                        control_port_number)
                );
        
            CREATE TABLE
                relationship__Affiliate__shares_iRODS__Host__provides__Service
                (
                    affiliate_Id,
                    host_provides_service_host_Id, 
                    host_provides_service_service_Id,
                    FOREIGN KEY (
                        affiliate_Id)
                        REFERENCES
                            entity__Affiliate(
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    FOREIGN KEY (
                        host_provides_service_host_Id,
                        host_provides_service_service_Id)
                        REFERENCES
                            gerund__Host__provides__Service(
                                host_Id,
                                service_Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    PRIMARY KEY (
                        affiliate_Id,
                        host_provides_service_host_Id,
                        host_provides_service_service_Id)
                );
        
            CREATE TABLE
                relationship__Private_User__uses_Shell__Host__provides__Service
                (
                    private_user_host_Id,
                    private_user_Id,
                    host_provides_service_host_Id,
                    host_provides_service_service_Id,
                    FOREIGN KEY (
                        private_user_host_Id,
                        private_user_Id)
                        REFERENCES
                            entity_weak__Private_User(
                                host_Id,
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    FOREIGN KEY (
                        host_provides_service_host_Id,
                        host_provides_service_service_Id)
                        REFERENCES
                            gerund__Host__provides__Service(
                                host_Id,
                                service_Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    PRIMARY KEY (
                        private_user_host_Id,
                        private_user_Id,
                        host_provides_service_host_Id,
                        host_provides_service_service_Id)
                );
        
            CREATE TABLE
                relationship__Private_Resource__belongs_to__Host__provides__Service
                (
                    private_resource_host_Id,
                    private_resource_private_user_Id,
                    private_resource_Id,
                    host_provides_service_host_Id,
                    host_provides_service_service_Id,
                    FOREIGN KEY (
                        private_resource_host_Id,
                        private_resource_private_user_Id,
                        private_resource_Id)
                        REFERENCES
                            entity_weak__Private_Resource(
                                host_Id,
                                private_user_Id,
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    FOREIGN KEY (
                        host_provides_service_host_Id,
                        host_provides_service_service_Id)
                        REFERENCES
                            gerund__Host__provides__Service(
                                host_Id,
                                service_Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    PRIMARY KEY (
                        private_resource_host_Id,
                        private_resource_private_user_Id,
                        private_resource_Id,
                        host_provides_service_host_Id,
                        host_provides_service_service_Id)
                );
                
            CREATE TABLE 
                relationship__Host__provides__Analytic_Application 
                (
                    host_Id, 
                    analytic_application_Id,
                    GUI_skeleton TEXT NOT NULL,
                    converted_script_path TEXT NOT NULL,
                    FOREIGN KEY (
                        host_Id) 
                        REFERENCES 
                            entity__Host(
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    FOREIGN KEY (
                        analytic_application_Id) 
                        REFERENCES 
                            entity__Analytic_Application(
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    PRIMARY KEY (
                        host_Id, 
                        analytic_application_Id)
                );
                
            CREATE TABLE 
                relationship__Analytic_Application__scheduled_by__Host__provides__Service
                (
                    analytic_application_Id,
                    host_provides_service_host_Id,
                    host_provides_service_service_Id,
                    tagged_script_path TEXT NOT NULL UNIQUE,
                    GUI_skeleton TEXT NOT NULL,
                    converted_script_path TEXT NOT NULL UNIQUE,
                    FOREIGN KEY (
                        analytic_application_Id) 
                        REFERENCES 
                            entity__analytic_application(
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    FOREIGN KEY (
                        host_provides_service_host_Id,
                        host_provides_service_service_Id) 
                        REFERENCES 
                            gerund__Host__provides__Service(
                                host_Id,
                                service_Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    PRIMARY KEY (
                        analytic_application_Id,
                        host_provides_service_host_Id,
                        host_provides_service_service_Id)
                );
                
            CREATE TABLE 
                relationship__BICLSU_User__has_jobs_on__Host 
                (
                    biclsu_user_Id, 
                    host_Id,
                    job_category TEXT NOT NULL,
                    job_Id TEXT NOT NULL,
                    FOREIGN KEY (
                        biclsu_user_Id) 
                        REFERENCES 
                            entity__BICLSU_User(
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    FOREIGN KEY (
                        host_Id) 
                        REFERENCES 
                            entity__Host(
                                Id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    PRIMARY KEY (
                        biclsu_user_Id, 
                        host_Id),
                    UNIQUE (
                        host_Id, 
                        job_category, 
                        job_Id) 
                        ON CONFLICT ROLLBACK
                )

        """
        self.db_operator.query_multiple(query_string)
    
    # end of function createDB
    
    def initializeDB(self):
    
        DB_Initializer(self.db_operator).initialize()
    
    # end of function initializeDB
    
    def displayDB(self):

        self.db_operator.displayAllTables()
    
    # end of function displayDB
    
    def getUserIdOf(self, user_account_list):
    
        return_value = {}
        
        for one_user_account in user_account_list:
        
            query = """
            
                SELECT
                    entity__BICLSU_User.Id
                FROM
                    entity__BICLSU_User
                WHERE
                    entity__BICLSU_User.myLSU_Id = ?
            
            """
            
            result_list = \
                self.db_operator.query_single(
                    query, 
                    (str(one_user_account),)
                )
            
            if result_list:
                return_value[one_user_account] = result_list[0][0]
            else:
                return_value[one_user_account] = None
        
        # end of for
        
        #print(str(return_value))
        return return_value
    
    # end of getUserIdOf
    
    ##
    # @brief    Retrive the Host Id and Private User Id of a Private Resource.
    #
    # @param    resource_Id_list    A list of Private Resource Ids.
    #
    # @return   A dictionary of resource Id and tuple pair.
    #           {<resc Id>: {"host": <host Id>, "user": <pvt usr Id>},...}
    def getHostUserIdOfResource(self, resource_Id_list):
    
        return_value = {}
    
        for one_Id in resource_Id_list:
        
            query = """
            
                SELECT
                    entity__Host.Id,
                    entity_weak__Private_User.Id
                FROM
                    entity__Host,
                    entity_weak__Private_User,
                    entity_weak__Private_Resource,
                    relationship_weak__Private_User__resides_on__Host,
                    relationship_weak__Private_User__has__Private_Resource
                WHERE
                    entity_weak__Private_Resource.Id = ?
                    AND
                    entity_weak__Private_Resource.host_Id = relationship_weak__Private_User__has__Private_Resource.private_resource_host_Id
                    AND
                    entity_weak__Private_Resource.private_user_Id = relationship_weak__Private_User__has__Private_Resource.private_resource_private_user_Id
                    AND
                    entity_weak__Private_Resource.Id = relationship_weak__Private_User__has__Private_Resource.private_resource_Id
                    AND
                    relationship_weak__Private_User__has__Private_Resource.private_user_host_Id = entity_weak__Private_User.host_Id
                    AND
                    relationship_weak__Private_User__has__Private_Resource.private_user_Id = entity_weak__Private_User.Id
                    AND
                    entity_weak__Private_User.host_Id = relationship_weak__Private_User__resides_on__Host.private_user_host_Id
                    AND
                    entity_weak__Private_User.Id = relationship_weak__Private_User__resides_on__Host.private_user_Id
                    AND
                    relationship_weak__Private_User__resides_on__Host.host_Id = entity__Host.Id
            """
            
            result_list = \
                self.db_operator.query_single(
                    query, 
                    (str(one_Id),)
                )
            
            if result_list:
                return_value[one_Id] = \
                    {"host": result_list[0][0], 
                    "user": result_list[0][1]}
            else:
                return_value[one_Id] = None
        
        # end of for
        
        print("ret val:" + str(return_value))
        return return_value
    
    # end of getHostUserIdOfResource
    
    def addUser(self, user_info_list):
    
        query_string = """
                
            INSERT INTO
                entity__BICLSU_User
                (
                    display_name, 
                    myLSU_Id, 
                    Elgg_user_Id, 
                    iRODS_user_Id,
                    iRODS_password,
                    human_admin
                )
            VALUES (?,?,?,?,?,?)
            """
            
        for one_info in user_info_list:
            
            self.db_operator.query_single(
                query_string, 
                (str(one_info["display_name"]),
                str(one_info["SSO_user"]),
                str(one_info["front_end_user"]),
                str(one_info["iRODS_user"]),
                str(one_info["iRODS_password"]),
                int(one_info["human_admin"])))
        
            # Save (commit) the changes
            self.db_operator.commit()
            
        # end of for
    
    # end of function addUser
    
    ##
    # @brief    Add a list of groups.
    #
    # @param    group_info_list A list of group information.
    #                           [{"display_name": <name>,
    #                           "front_end_group": <front end group Id>,
    #                           "front_end_creator": <front end creator Id>,
    #                           "iRODS_group"}, ...]
    #
    # @return   A list of created group information.
    #           [{"group": <group Id>,
    #           "front_end_group": <front end group Id>,
    #           "front_end_creator": <front end creator Id>}, ...]
    #
    def addGroup(self, group_info_list):
    
        query_string_add_group = """
                
            INSERT INTO
                entity__BICLSU_Group
                (
                    display_name,  
                    Elgg_group_Id, 
                    iRODS_group_Id
                )
            VALUES (?,?,?)
            """
            
        query_string_get_group_Id = """
                
            SELECT
                entity__BICLSU_Group.Id
            FROM
                entity__BICLSU_Group
            WHERE
                entity__BICLSU_Group.Elgg_group_Id = ?   
            """
            
        return_value = []
            
        for one_info in group_info_list:
            
            self.db_operator.query_single(
                query_string_add_group, 
                (str(one_info["display_name"]),
                str(one_info["front_end_group"]),
                str(one_info["iRODS_group"])))
            # Save (commit) the changes
            self.db_operator.commit()
            
            result = \
                self.db_operator.query_single(
                    query_string_get_group_Id, 
                    (one_info["front_end_group"],))
                    
            return_value.append(
                {"group": result[0][0],
                "front_end_group": one_info["front_end_group"],
                "front_end_creator": one_info["front_end_creator"]})
        
        # end of for
        
        #print("ret val: " + str(return_value))
        return return_value
    
    # end of function addGroup
    
    def listGroup(self, user_Id_list):
    
        query_string = """
                
            SELECT
                entity__BICLSU_Group.Id,
                entity__BICLSU_Group.display_name
            FROM
                entity__BICLSU_User,
                entity__BICLSU_Group,
                relationship__BICLSU_Group__consists_of__BICLSU_User
            WHERE
                entity__BICLSU_User.myLSU_Id = ?
                AND
                entity__BICLSU_User.Id = relationship__BICLSU_Group__consists_of__BICLSU_User.biclsu_user_Id
                AND
                relationship__BICLSU_Group__consists_of__BICLSU_User.biclsu_group_Id = entity__BICLSU_Group.Id
            ORDER BY
                entity__BICLSU_Group.display_name
                ASC
            """
            
        return_value = {}
            
        for one_Id in user_Id_list:   
            result = \
                self.db_operator.query_single(
                    query_string, 
                    (str(one_Id),))
                    
            if result:
                return_value[one_Id] = []
                for one_result in result:
                    return_value[one_Id].append(
                        {"Id": one_result[0],
                        "display_name": one_result[1]})
            else:
                return_value[one_Id] = []
                    
        # end of for
                
        #print("ret val: " + str(return_value))
        return return_value
    
    # end of function listGroup
    
    def editGroup(self, group_info_list):
    
        query_string = """
                
            UPDATE
                entity__BICLSU_Group
            SET
                display_name = ?
            WHERE
                entity__BICLSU_Group.Elgg_group_Id = ?
            """
            
        for one_info in group_info_list: 
          
            self.db_operator.query_single(
                query_string, 
                (one_info["display_name"],
                int(one_info["front_end_group"])))
                    
        # end of for
    
    # end of function editGroup
    
    def removeGroup(self, group_info_list):
    
        query_string = """
                
            DELETE FROM
                entity__BICLSU_Group
            WHERE
                entity__BICLSU_Group.Elgg_group_Id = ?
            """
            
        for one_info in group_info_list: 
          
            self.db_operator.query_single(
                query_string, 
                (str(one_info["front_end_group"]),))
                
            # TO DO
            #cascadingly clean relative tables
                    
        # end of for
    
    # end of function removeGroup
       
    def getFriendship(self, user_list):
    
        return_value = {}
    
        for one_user in user_list:
        
            query_string = """
        
                SELECT
                    entity__BICLSU_User_2.Id AS User_Id,
                    entity__BICLSU_User_2.display_name
                FROM
                    entity__BICLSU_User AS entity__BICLSU_User_1,
                    entity__BICLSU_User AS entity__BICLSU_User_2,
                    relationship__BICLSU_User__is_a_friend_of__BICLSU_User
                WHERE
                    entity__BICLSU_User_1.Id = ? 
                    AND 
                    entity__BICLSU_User_1.Id =
                        relationship__BICLSU_User__is_a_friend_of__BICLSU_User.biclsu_user_Id_small
                    AND 
                    relationship__BICLSU_User__is_a_friend_of__BICLSU_User.biclsu_user_Id_large =
                        entity__BICLSU_User_2.Id
                UNION
                SELECT
                    entity__BICLSU_User_1.Id AS User_Id,
                    entity__BICLSU_User_1.display_name
                FROM
                    entity__BICLSU_User AS entity__BICLSU_User_1,
                    entity__BICLSU_User AS entity__BICLSU_User_2,
                    relationship__BICLSU_User__is_a_friend_of__BICLSU_User
                WHERE
                    entity__BICLSU_User_2.Id = ? 
                    AND 
                    entity__BICLSU_User_1.Id =
                        relationship__BICLSU_User__is_a_friend_of__BICLSU_User.biclsu_user_Id_small
                    AND 
                    relationship__BICLSU_User__is_a_friend_of__BICLSU_User.biclsu_user_Id_large =
                        entity__BICLSU_User_2.Id
                ORDER BY
                    User_Id
                    ASC
            """
            
            result = \
                self.db_operator.query_single(
                    query_string, 
                    (str(one_user), str(one_user))
                )
            
            friend_list = []
            
            #print("")
            #print("All friends for " + str(one_user))
            if result:
                for one_record in result:
                    friend_list.append(one_record)
                    #print(str(one_record))
            return_value[str(one_user)] = friend_list
            
        # end of for
        
        #print(str(return_value))
        return return_value
    
    # end of function getFriendshipOf
    
    ##
    # @brief    Add a list of memberships.
    #
    # @param    membership_list A list of memberships.
    #                           [{"front_end_group": <front end group Id>,
    #                           "front_end_user": <front end user Id>, 
    #                           "is_admin": "0"/"1"}, ...]
    #
    def addMember(self, membership_list):
    
        query_string_get_group_Id = """
        
            SELECT
                entity__BICLSU_Group.Id
            FROM
                entity__BICLSU_Group
            WHERE
                entity__BICLSU_Group.Elgg_group_Id = ?
            """
            
        query_string_get_user_Id = """
        
            SELECT
                entity__BICLSU_User.Id
            FROM
                entity__BICLSU_User
            WHERE
                entity__BICLSU_User.Elgg_user_Id = ?
            """    
    
        query_string = """
                
            INSERT INTO
                relationship__BICLSU_Group__consists_of__BICLSU_User
                (
                    biclsu_group_Id, 
                    biclsu_user_Id,
                    is_admin
                )
            VALUES (?,?,?)
            """
    
        for one_membership in membership_list:
            
            result = \
                self.db_operator.query_single(
                    query_string_get_group_Id, 
                    (str(one_membership["front_end_group"]),))
            group_Id = result[0][0]
            
            result = \
                self.db_operator.query_single(
                    query_string_get_user_Id, 
                    (str(one_membership["front_end_user"]),))
            user_Id = result[0][0]
            
            self.db_operator.query_single(
                query_string, 
                (int(group_Id),
                int(user_Id),
                int(one_membership["is_admin"])))
        
        # Save (commit) the changes
        self.db_operator.commit()
    
    # end of function addMember
    
    def listMember(self, group_list):
    
        pass
        
    # end of function listMember
    
    def removeMember(self, membership_info_list):
    
        query_string_get_group_Id = """
        
            SELECT
                entity__BICLSU_Group.Id
            FROM
                entity__BICLSU_Group
            WHERE
                entity__BICLSU_Group.Elgg_group_Id = ?
            """
            
        query_string_get_user_Id = """
        
            SELECT
                entity__BICLSU_User.Id
            FROM
                entity__BICLSU_User
            WHERE
                entity__BICLSU_User.Elgg_user_Id = ?
            """
    
        query_string = """
                
            DELETE FROM
                relationship__BICLSU_Group__consists_of__BICLSU_User
            WHERE
                relationship__BICLSU_Group__consists_of__BICLSU_User.biclsu_group_Id = ?
                AND
                relationship__BICLSU_Group__consists_of__BICLSU_User.biclsu_user_Id = ?
            """
            
        for one_info in membership_info_list: 
          
            result = \
                self.db_operator.query_single(
                    query_string_get_group_Id, 
                    (str(one_info["front_end_group"]),))
            group_Id = result[0][0]
          
            result = \
                self.db_operator.query_single(
                    query_string_get_user_Id, 
                    (str(one_info["front_end_user"]),))
            user_Id = result[0][0]
          
            self.db_operator.query_single(
                query_string, 
                (group_Id,
                user_Id))
                    
        # end of for
    
    # end of function removeMember
    
    def listMembership(self, user_list):
    
        return_value = {}
    
        for one_user in user_list:
        
            query = """
            
                SELECT
                    entity__BICLSU_Group.Id,
                    entity__BICLSU_Group.display_name
                FROM
                    entity__BICLSU_User,
                    entity__BICLSU_Group,
                    relationship__BICLSU_Group__consists_of__BICLSU_User
                WHERE
                    entity__BICLSU_User.Id = ?
                    AND 
                    entity__BICLSU_User.Id = 
                        relationship__BICLSU_Group__consists_of__BICLSU_User.biclsu_user_Id
                    AND
                    relationship__BICLSU_Group__consists_of__BICLSU_User.biclsu_group_Id = 
                        entity__BICLSU_Group.Id
                ORDER BY
                    entity__BICLSU_Group.Id
                    ASC
            """
            
            result = self.db_operator.query_single(query, (str(one_user),))
            
            group_list = []
            
            #print("")
            #print("All memberships")
            if result:
                for one_record in result:
                    group_list.append(one_record)
                    #print(str(one_record))
            return_value[str(one_user)] = group_list    
                
        # end of for
        
        #print(str(return_value))
        return return_value
    
    # end of function getMembership
    
    def addResource(self, resource_info_list):
    
        return_value = {}
    
        for one_info in resource_info_list:
        
            self.db_operator.transaction_begin()
        
            try:
        
                query_string = """
                        SELECT
                            MAX(entity_weak__Private_User.Id)
                        FROM
                            entity_weak__Private_User
                    """
                result = \
                    self.db_operator.query_single(
                        query_string)
                
                private_user_Id = result[0][0] + 1
                #print("new pvt id: " + str(private_user_Id))
            
                query_string = """
                    INSERT INTO
                        entity_weak__Private_User
                        (
                            host_Id, 
                            Id, 
                            private_user_account
                        )
                    VALUES (?,?,?)
                    """
                self.db_operator.query_single(
                    query_string, 
                    (int(one_info["host"]),
                    private_user_Id,
                    str(one_info["private_user"])))
                    
                query_string = """
                    INSERT INTO
                        relationship__BICLSU_User__maps_to__Private_User
                        (
                            biclsu_user_Id, 
                            private_user_host_Id, 
                            private_user_Id
                        )
                    VALUES (?,?,?)
                    """
                self.db_operator.query_single(
                    query_string, 
                    (int(one_info["biclsu_user"]),
                    int(one_info["host"]),
                    private_user_Id))
                    
                query_string = """
                    INSERT INTO
                        relationship_weak__Private_User__resides_on__Host
                        (
                            private_user_host_Id, 
                            private_user_Id, 
                            host_Id
                        )
                    VALUES (?,?,?)
                    """
                self.db_operator.query_single(
                    query_string, 
                    (int(one_info["host"]),
                    private_user_Id,
                    int(one_info["host"])))
                    
                query_string = """
                    INSERT INTO
                        entity__Control_Credential
                        (
                            category,
                            path,
                            passphrase
                        )
                    VALUES (?,?,?)
                    """
                self.db_operator.query_single(
                    query_string, 
                    (one_info["credential_category"],
                    one_info["credential_path"],
                    ""))
                    
                query_string = """
                    SELECT
                        entity__Control_Credential.Id
                    FROM
                        entity__Control_Credential
                    WHERE
                        entity__Control_Credential.path = ?
                    """
                result = \
                    self.db_operator.query_single(
                        query_string, 
                        (one_info["credential_path"],))
                
                control_credential_Id = result[0][0]
                
                query_string = """
                    INSERT INTO
                        relationship__Private_User__accepts__Control_Credential
                        (
                            private_user_host_Id,
                            private_user_Id,
                            control_credential_Id
                        )
                    VALUES (?,?,?)
                    """
                self.db_operator.query_single(
                    query_string, 
                    (int(one_info["host"]),
                    private_user_Id,
                    control_credential_Id))
                    
                # TO DO
                # make shell an optional parameter
                query_string = """
                    INSERT INTO
                        relationship__Private_User__uses_Shell__Host__provides__Service
                        (
                            private_user_host_Id,
                            private_user_Id,
                            host_provides_service_host_Id, 
                            host_provides_service_service_Id
                        )
                    VALUES (?,?,?,?)
                    """
                self.db_operator.query_single(
                    query_string, 
                    (int(one_info["host"]),
                    private_user_Id,
                    int(one_info["host"]),
                    int(one_info["shell"])))
                    
                return_value[\
                    str((one_info["host"], one_info["private_user"]))] = \
                        {"public_key": one_info["public_key"]}
                    
            except sqlite3.IntegrityError as e:
                traceback.print_exc()
                print("duplicated user registration!")
                # retrieve existing private user Id
                query_string = """
                
                    SELECT
                        entity_weak__Private_User.Id
                    FROM
                        entity_weak__Private_User
                    WHERE
                        entity_weak__Private_User.host_Id = ?
                        AND
                        entity_weak__Private_User.private_user_account = ?
                    """
                result = \
                    self.db_operator.query_single(
                        query_string, 
                        (int(one_info["host"]),
                        str(one_info["private_user"])))
                private_user_Id = result[0][0]
                #print("existing pvt id: " + str(private_user_Id))
                
                return_value[\
                    str((one_info["host"], one_info["private_user"]))] = \
                        {"old_credential_path": one_info["credential_path"]}
                    
            query_string = """
                    SELECT
                        MAX(entity_weak__Private_Resource.Id)
                    FROM
                        entity_weak__Private_Resource
                """
            result = \
                self.db_operator.query_single(
                    query_string)
            
            private_resource_Id = result[0][0] + 1
            #print("new pvt resc id: " + str(private_resource_Id))
                
            query_string = """
                INSERT INTO
                    entity_weak__Private_Resource
                    (
                        host_Id, 
                        private_user_Id,
                        Id, 
                        display_name, 
                        root_absolute_path,
                        allow_external_acl
                    )
                VALUES (?,?,?,?,?,?)
                """
            self.db_operator.query_single(
                query_string, 
                (int(one_info["host"]),
                private_user_Id,
                private_resource_Id,
                one_info["display_name"],
                one_info["root"],
                int(one_info["allow_ACL"])))
                
            query_string = """
                INSERT INTO
                    relationship_weak__Private_User__has__Private_Resource
                    (
                        private_user_host_Id,
                        private_user_Id, 
                        private_resource_host_Id, 
                        private_resource_private_user_Id,
                        private_resource_Id
                    )
                VALUES (?,?,?,?,?)
                """
            self.db_operator.query_single(
                query_string, 
                (int(one_info["host"]),
                private_user_Id,
                int(one_info["host"]),
                private_user_Id,
                private_resource_Id))
                
            query_string = """
                INSERT INTO
                    relationship_weak__Private_Resource__resides_on__Host
                    (
                        private_resource_host_Id,
                        private_resource_private_user_Id, 
                        private_resource_Id,
                        host_Id
                    )
                VALUES (?,?,?,?)
                """
            self.db_operator.query_single(
                query_string, 
                (int(one_info["host"]),
                private_user_Id,
                private_resource_Id,
                int(one_info["host"])))
                
            #query_string = """
            #        SELECT
            #            *
            #        FROM
            #            gerund__Host__provides__Service
            #        WHERE
            #            gerund__Host__provides__Service.host_Id = ?
            #            AND
            #            gerund__Host__provides__Service.service_Id = 21
            #    """
            #result = \
            #    self.db_operator.query_single(
            #        query_string,
            #        (int(one_info["host"]),))
            #print("host svc: " + str(result))
                
            query_string = """
                INSERT INTO
                    relationship__Private_Resource__belongs_to__Host__provides__Service
                    (
                        private_resource_host_Id,
                        private_resource_private_user_Id,
                        private_resource_Id,
                        host_provides_service_host_Id, 
                        host_provides_service_service_Id
                    )
                VALUES (?,?,?,?,?)
                """
            self.db_operator.query_single(
                query_string, 
                (int(one_info["host"]),
                private_user_Id,
                private_resource_Id,
                int(one_info["host"]),
                int(one_info["service"])))
        
            self.db_operator.transaction_commit()
        
        # end of for
    
        print("ret val: " + str(return_value))
        return return_value
    
    # end of function addResource
    
    def addResource_old(self, resource_info_list):
    
        return_value = {}
    
        for one_info in resource_info_list:
        
            self.db_operator.transaction_begin()
        
            try:
        
                query_string = """
                        SELECT
                            MAX(entity_weak__Private_User.Id)
                        FROM
                            entity_weak__Private_User
                    """
                result = \
                    self.db_operator.query_single(
                        query_string)
                
                private_user_Id = result[0][0] + 1
                #print("new pvt id: " + str(private_user_Id))
            
                query_string = """
                    INSERT INTO
                        entity_weak__Private_User
                        (
                            host_Id, 
                            Id, 
                            private_user_account
                        )
                    VALUES (?,?,?)
                    """
                self.db_operator.query_single(
                    query_string, 
                    (one_info["host"],
                    private_user_Id,
                    one_info["private_user"]))
                    
                query_string = """
                        SELECT
                            MAX(entity_weak__Private_Resource.Id)
                        FROM
                            entity_weak__Private_Resource
                    """
                result = \
                    self.db_operator.query_single(
                        query_string)
                
                private_resource_Id = result[0][0] + 1
                #print("new pvt resc id: " + str(private_resource_Id))
                    
                query_string = """
                    INSERT INTO
                        entity_weak__Private_Resource
                        (
                            host_Id, 
                            private_user_Id,
                            Id, 
                            display_name, 
                            root_absolute_path,
                            allow_external_acl
                        )
                    VALUES (?,?,?,?,?,?)
                    """
                self.db_operator.query_single(
                    query_string, 
                    (one_info["host"],
                    private_user_Id,
                    private_resource_Id,
                    one_info["display_name"],
                    one_info["root"],
                    one_info["allow_ACL"]))
                    
                query_string = """
                    INSERT INTO
                        relationship__BICLSU_User__maps_to__Private_User
                        (
                            biclsu_user_Id, 
                            private_user_host_Id, 
                            private_user_Id
                        )
                    VALUES (?,?,?)
                    """
                self.db_operator.query_single(
                    query_string, 
                    (one_info["biclsu_user"],
                    one_info["host"],
                    private_user_Id))
                    
                query_string = """
                    INSERT INTO
                        relationship_weak__Private_User__resides_on__Host
                        (
                            private_user_host_Id, 
                            private_user_Id, 
                            host_Id
                        )
                    VALUES (?,?,?)
                    """
                self.db_operator.query_single(
                    query_string, 
                    (one_info["host"],
                    private_user_Id,
                    one_info["host"]))
                    
                query_string = """
                    INSERT INTO
                        relationship_weak__Private_User__has__Private_Resource
                        (
                            private_user_host_Id,
                            private_user_Id, 
                            private_resource_host_Id, 
                            private_resource_private_user_Id,
                            private_resource_Id
                        )
                    VALUES (?,?,?,?,?)
                    """
                self.db_operator.query_single(
                    query_string, 
                    (one_info["host"],
                    private_user_Id,
                    one_info["host"],
                    private_user_Id,
                    private_resource_Id))
                    
                query_string = """
                    INSERT INTO
                        relationship_weak__Private_Resource__resides_on__Host
                        (
                            private_resource_host_Id,
                            private_resource_private_user_Id, 
                            private_resource_Id,
                            host_Id
                        )
                    VALUES (?,?,?,?)
                    """
                self.db_operator.query_single(
                    query_string, 
                    (one_info["host"],
                    private_user_Id,
                    private_resource_Id,
                    one_info["host"]))
                    
                #query_string = """
                #        SELECT
                #            *
                #        FROM
                #            gerund__Host__provides__Service
                #        WHERE
                #            gerund__Host__provides__Service.host_Id = ?
                #            AND
                #            gerund__Host__provides__Service.service_Id = 21
                #    """
                #result = \
                #    self.db_operator.query_single(
                #        query_string,
                #        (int(one_info["host"]),))
                #print("host svc: " + str(result))
                
                    
                query_string = """
                    INSERT INTO
                        relationship__Private_Resource__belongs_to__Host__provides__Service
                        (
                            private_resource_host_Id,
                            private_resource_private_user_Id,
                            private_resource_Id,
                            host_provides_service_host_Id, 
                            host_provides_service_service_Id
                        )
                    VALUES (?,?,?,?,?)
                    """
                self.db_operator.query_single(
                    query_string, 
                    (one_info["host"],
                    private_user_Id,
                    private_resource_Id,
                    int(one_info["host"]),
                    int(one_info["service"])))
                  
                query_string = """
                    INSERT INTO
                        entity__Control_Credential
                        (
                            category,
                            path,
                            passphrase
                        )
                    VALUES (?,?,?)
                    """
                self.db_operator.query_single(
                    query_string, 
                    (one_info["credential_category"],
                    one_info["credential_path"],
                    ""))
                    
                query_string = """
                    SELECT
                        entity__Control_Credential.Id
                    FROM
                        entity__Control_Credential
                    WHERE
                        entity__Control_Credential.path = ?
                    """
                result = \
                    self.db_operator.query_single(
                        query_string, 
                        (one_info["credential_path"],))
                
                control_credential_Id = result[0][0]
                
                query_string = """
                    INSERT INTO
                        relationship__Private_User__accepts__Control_Credential
                        (
                            private_user_host_Id,
                            private_user_Id,
                            control_credential_Id
                        )
                    VALUES (?,?,?)
                    """
                self.db_operator.query_single(
                    query_string, 
                    (one_info["host"],
                    private_user_Id,
                    control_credential_Id))
                    
                # TO DO
                # make shell an optional parameter
                query_string = """
                    INSERT INTO
                        relationship__Private_User__uses_Shell__Host__provides__Service
                        (
                            private_user_host_Id,
                            private_user_Id,
                            host_provides_service_host_Id, 
                            host_provides_service_service_Id
                        )
                    VALUES (?,?,?,?)
                    """
                self.db_operator.query_single(
                    query_string, 
                    (one_info["host"],
                    private_user_Id,
                    int(one_info["host"]),
                    int(one_info["shell"])))
                    
                return_value[\
                    str((one_info["host"], one_info["private_user"]))] = \
                        {"public_key": one_info["public_key"]}
                    
            except sqlite3.IntegrityError as e:
                traceback.print_exc()
                print("duplicated registration!")
                
                # TO DO
                #design a cross-platform solution
                # TO DO
                #distinguish duplication in Private_User 
                #from exceptions from other tables
                #self.db_operator.transaction_rollback()
                
                query_string = """
                    SELECT
                        entity_weak__Private_User.Id
                    FROM
                        entity_weak__Private_User
                    WHERE
                        entity_weak__Private_User.host_Id = ?
                        AND
                        entity_weak__Private_User.private_user_account = ?
                    """
                result = \
                    self.db_operator.query_single(
                        query_string,
                        (one_info["host"],
                        one_info["private_user"]))
                
                private_user_Id = result[0][0]
                
                # find existing ctl cred Id and path
                # TO DO
                #use complete query
                query_string = """
                
                    SELECT
                        entity__Control_Credential.Id,
                        entity__Control_Credential.path
                    FROM
                        entity__Control_Credential,
                        relationship__Private_User__accepts__Control_Credential
                    WHERE
                        relationship__Private_User__accepts__Control_Credential.private_user_host_Id = ?
                        AND
                        relationship__Private_User__accepts__Control_Credential.private_user_Id = ?
                        AND
                        entity__Control_Credential.Id = relationship__Private_User__accepts__Control_Credential.control_credential_Id
                        AND
                        entity__Control_Credential.category = ?
                    """
                result = \
                    self.db_operator.query_single(
                        query_string, 
                        (one_info["host"],
                        private_user_Id,
                        one_info["credential_category"]))
                #print("old cred: " + str(result))
                
                old_credential_path = result[0][1]
                
                # preserve old cred path for cleaning
                return_value[\
                    str((one_info["host"], one_info["private_user"]))] = \
                        {"old_credential_path": old_credential_path,
                        "public_key": one_info["public_key"]}
                
                query_string = """
                
                    UPDATE
                        entity__Control_Credential
                    SET
                        category = ?,
                        path = ?
                    WHERE
                        entity__Control_Credential.Id = ?
                    """
                self.db_operator.query_single(
                    query_string, 
                    (one_info["credential_category"],
                    one_info["credential_path"],
                    result[0][0]))
                    
                print("ctl cred renewed!")
            
            self.db_operator.transaction_commit()
        
        # end of for
    
        print("ret val: " + str(return_value))
        return return_value
    
    # end of function addResource
    
    ##
    # @brief    List all resources of a type of a user.
    #
    # @param    list_info_list  A list of dictionaries.
    #                           [{"user": <SSO Id>,
    #                           "category":[<resc category>, ...]}, ...]
    #                           
    # @return   A dictionary.
    #           {<SSO Id>: [
    #           {"category": <cat>, 
    #           "resource": <resc Id>, "display_name":<name>, 
    #           "user": <user Id>, "host": <host Id>, "service": <svc Id>}, 
    #           ...]}
    #
    def listResource(self, list_info_list):
    
        return_value = {}
    
        # TO DO
        # use complete query
        query = """
        
            SELECT
                entity__Service.category,
                entity_weak__Private_Resource.Id,
                entity_weak__Private_Resource.display_name,
                entity__BICLSU_User.Id,
                gerund__Host__provides__Service.host_Id,
                gerund__Host__provides__Service.service_Id
            FROM
                entity__BICLSU_User,
                entity_weak__Private_User,
                entity_weak__Private_Resource,
                entity__Service,
                gerund__Host__provides__Service,
                relationship__BICLSU_User__maps_to__Private_User,
                relationship_weak__Private_User__has__Private_Resource,
                relationship__Private_Resource__belongs_to__Host__provides__Service
            WHERE
                entity__BICLSU_User.myLSU_Id = ?
                AND
                entity__Service.category
                    IN ({})
                AND
                entity__BICLSU_User.Id = relationship__BICLSU_User__maps_to__Private_User.biclsu_user_Id
                AND
                relationship__BICLSU_User__maps_to__Private_User.private_user_host_Id = entity_weak__Private_User.host_Id
                AND
                relationship__BICLSU_User__maps_to__Private_User.private_user_Id = entity_weak__Private_User.Id
                AND
                entity_weak__Private_User.host_Id = relationship_weak__Private_User__has__Private_Resource.private_user_host_Id
                AND
                entity_weak__Private_User.Id = relationship_weak__Private_User__has__Private_Resource.private_user_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_host_Id = entity_weak__Private_Resource.host_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_private_user_Id = entity_weak__Private_Resource.private_user_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_Id = entity_weak__Private_Resource.Id
                AND
                entity_weak__Private_Resource.host_Id = relationship__Private_Resource__belongs_to__Host__provides__Service.private_resource_host_Id
                AND
                entity_weak__Private_Resource.private_user_Id = relationship__Private_Resource__belongs_to__Host__provides__Service.private_resource_private_user_Id
                AND
                entity_weak__Private_Resource.Id = relationship__Private_Resource__belongs_to__Host__provides__Service.private_resource_Id
                AND
                relationship__Private_Resource__belongs_to__Host__provides__Service.host_provides_service_host_Id = gerund__Host__provides__Service.host_Id
                AND
                relationship__Private_Resource__belongs_to__Host__provides__Service.host_provides_service_service_Id = gerund__Host__provides__Service.service_Id
                AND
                gerund__Host__provides__Service.service_Id = entity__Service.Id
            """
    
        for one_info in list_info_list:
        
            parameter = []
            parameter.append(one_info["user"])
            for one_category in one_info["category"]:
                parameter.append(one_category)
            
            result = \
                self.db_operator.query_single(
                    query.format(','.join('?' for i in one_info["category"])), 
                    parameter)
            #print("all resc: " + str(result))
            
            return_value[one_info["user"]] = []
            for one_result in result:
                return_value[one_info["user"]].append(
                    {"category": one_result[0],
                    "resource": one_result[1],
                    "display_name": one_result[2],
                    "user": one_result[3],
                    "host": one_result[4],
                    "service": one_result[5]})
            
        # end of for
        
        return return_value
    
    # end of function listResource
    
    def listServiceOnHostWithCategory(self, host_Id, service_category_list):
    
        query = """
        
            SELECT
                entity__Service.Id,
                entity__Service.server_type,
                entity__Service.server_version
            FROM
                entity__Host,
                entity__Service,
                gerund__Host__provides__Service
            WHERE
                entity__Service.category 
                    IN ({})
                AND
                entity__Host.Id = ?
                AND
                entity__Host.Id =
                    gerund__Host__provides__Service.host_Id
                AND
                gerund__Host__provides__Service.service_Id = 
                    entity__Service.Id
        
        """.format(','.join('?' for i in service_category_list))
        
        argument_list = [category for category in service_category_list]
        argument_list.append(host_Id)
        result_list = \
            self.db_operator.query_single(
                    query,
                    argument_list
                )
                         
        if not result_list:
            return_value = None
        else:
            return_value = {"host": host_Id}
            return_value["service"] = []
            for one_service in result_list:
                return_value["service"].append(
                    {"ID": one_service[0],
                    "type": one_service[1],
                    "version": one_service[2]})
        
        #print("ret val: " + str(return_value))
        
        return return_value
    
    # end of function 
    
    def getDefaultPathOf(self, user_Id_list):
    
        default_path = {}
        
        for one_user_Id in user_Id_list:
        
            query = """
            
                SELECT
                    entity_weak__Private_Resource.Id,
                    entity_weak__Private_Resource.root_absolute_path
                FROM
                    entity__BICLSU_User,
                    entity_weak__Private_Resource,
                    relationship__BICLSU_User__frequently_accesses__Private_Resource
                WHERE
                    entity__BICLSU_User.Id = ?
                    AND
                    entity__BICLSU_User.Id = relationship__BICLSU_User__frequently_accesses__Private_Resource.biclsu_user_Id
                    AND
                    relationship__BICLSU_User__frequently_accesses__Private_Resource.private_resource_host_Id = entity_weak__Private_Resource.host_Id
                    AND relationship__BICLSU_User__frequently_accesses__Private_Resource.private_resource_private_user_Id = entity_weak__Private_Resource.private_user_Id
                    AND
                    relationship__BICLSU_User__frequently_accesses__Private_Resource.private_resource_Id = entity_weak__Private_Resource.Id
            
            """
            
            result_list = \
                self.db_operator.query_single(
                    query, 
                    (str(one_user_Id),)
                )
            #print("result: " + str(result_list))
        
            if not result_list:
                default_path[one_user_Id] = None
            else:
                default_path[one_user_Id] = []
                
                for one_result in result_list:
                
                    if one_result[1] == "":
                        category = "computing"
                    else:
                        category = "storage"
                
                    path = \
                        "/" + str(one_user_Id) + "/" + str(one_result[0]) + "/"
                    # TO DO: change below to prepend /<autonomous region>
                    # to default_path
                    path = "/0" + path
                        
                    default_path[one_user_Id].append((category, path))
        
        # end of for
        
        print("ret def path: " + str(default_path))
        return default_path
    
    # end of function getDefaultPathOf
    
    def isPathAccessible(self, user, path_list):
    
        pass
        
    # end of function isPathsAccessibleForAUser
    
    def isNarrationAccessible(self, user, key_list):
    
        pass
    
    # end of function isNarrationAccessibleForAUser
    
    def isPathACLMutable(self, user, path_list):
    
        pass
        
    # end of function isPathACLMutable
    
    def getHostOfResource(self, resource_list):
    
        pass
        
    # end of function getHostOfResource
    
    def getPartnerSystemInformation(self, logical_path_segments):
        
        #print("logical seg: " + str(logical_path_segments))
    
        autonomous_region_Id = logical_path_segments[0]
        user_Id = logical_path_segments[1]
        resource_Id = logical_path_segments[2]
        
        #print("auto region id: " + str(autonomous_region_Id))
        #print("user id: " + str(user_Id))
        #print("resource id: " + str(resource_Id))
        
        one_path_information = {}

        #{resource Id: <resource Id>,
        #resource category: <resource category>,
        #resource server type: <resource server type>,
        #resource server version: <resource server version>,
        #iRODS password: <BIC-LSU iRODS password>,
        #private user: <user>,
        #network interface:[
        #   {network: <network type>,
        #   IPv4 interface: <IPv4 or FQDN>,
        #   IPv6 interface: <IPv6 or FQDN>,
        #   capacity: <capacity>}
        #   ]
        #service:{
        #   <category>:{
        #       <server type>: {
        #           <server version>: [
        #               {network: <network type>,
        #               IPv4 interface: <IPv4 or FQDN>,
        #               IPv6 interface: <IPv6 or FQDN>,
        #               capacity: <capacity>,
        #               port: <port>}
        #               ]
        #       }
        #   }
        #}
        total_pairs_in_final_interface_list_element = 5
        # === optional block begin ===
        #shell type: <shell type>,
        #shell version: <shell version>,
        # === optional block end ===
        # === optional block begin ===
        #control credential: [
        #   {category:<category>,
        #   path:<path>,
        #   passphrase:<passphrase>}
        #   ]
        # === optional block end ===
        # === optional block begin ===
        #root path: <root path>
        # === optional block end ===
        # === optional block begin ===
        # sorted list
        #transfer application:[
        #   [<priority>, <application Id>, <parameters ...>],
        #   ...
        #]
        # === optional block end ===
        # === optional block begin ===
        # include the resources
        # which contains the credential for accessing another resource 
        # === optional block end ===
        
        # retrieve private resource info
        
        one_path_information["resource Id"] = str(resource_Id)
        
        query = """
        
            SELECT
                entity__Service.category,
                entity__Service.server_type,
                entity__Service.server_version
            FROM
                entity_weak__Private_Resource,
                entity__Service,
                gerund__Host__provides__Service,
                relationship__Private_Resource__belongs_to__Host__provides__Service
            WHERE
                entity_weak__Private_Resource.Id = ?
                AND
                entity_weak__Private_Resource.host_Id = relationship__Private_Resource__belongs_to__Host__provides__Service.private_resource_host_Id
                AND
                entity_weak__Private_Resource.Id = relationship__Private_Resource__belongs_to__Host__provides__Service.private_resource_Id
                AND relationship__Private_Resource__belongs_to__Host__provides__Service.host_provides_service_host_Id = gerund__Host__provides__Service.host_Id
                AND relationship__Private_Resource__belongs_to__Host__provides__Service.host_provides_service_service_Id = gerund__Host__provides__Service.service_Id
                AND
                entity__Service.Id = gerund__Host__provides__Service.service_Id
        
        """
        
        result_list = \
            self.db_operator.query_single(query, (str(resource_Id),))
        
        if not result_list:
            return None
        else:
            one_path_information["resource category"] = \
                result_list[0][0]
            one_path_information["resource server type"] = \
                result_list[0][1]
            one_path_information["resource server version"] = \
                result_list[0][2]

        #print("one path information: " + str(one_path_information))
        
        # retrieve
        # default iRODS password
        # private user
        query = """
            
            SELECT
                entity__BICLSU_user.iRODS_password,
                entity_weak__Private_User.private_user_account
            FROM
                entity__BICLSU_user,
                entity_weak__Private_User,
                entity_weak__Private_Resource,
                relationship__BICLSU_User__maps_to__Private_User,
                relationship_weak__Private_User__has__Private_Resource
            WHERE
                entity__BICLSU_User.Id = ?
                AND
                entity_weak__Private_Resource.Id = ?
                AND
                entity__BICLSU_User.Id = relationship__BICLSU_User__maps_to__Private_User.biclsu_user_Id
                AND
                relationship__BICLSU_User__maps_to__Private_User.private_user_host_Id = entity_weak__Private_User.host_Id
                AND
                relationship__BICLSU_User__maps_to__Private_User.private_user_Id = entity_weak__Private_User.Id
                AND
                entity_weak__Private_User.host_Id = relationship_weak__Private_User__has__Private_Resource.private_user_host_Id
                AND
                entity_weak__Private_User.Id = relationship_weak__Private_User__has__Private_Resource.private_user_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_host_Id = entity_weak__Private_Resource.host_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_Id = entity_weak__Private_Resource.Id
        """
        
        result_list = \
            self.db_operator.query_single(
                query, 
                (str(user_Id), str(resource_Id))
            )
        
        if result_list:
            one_path_information["iRODS password"] = result_list[0][0]
            one_path_information["private user"] = result_list[0][1]
        
        #print("one_path_information: " + str(one_path_information))
            
        # retrieve network interface 
        query = """
        
            SELECT
                entity_weak__Network_Interface.network_type,
                entity_weak__Network_Interface.IPv4_or_FQDN,
                entity_weak__Network_Interface.IPv6_or_FQDN,
                entity_weak__Network_Interface.capacity
            FROM
                entity__BICLSU_user,
                entity_weak__Private_User,
                entity_weak__Private_Resource,
                entity__Host,
                entity_weak__Network_Interface,
                relationship__BICLSU_User__maps_to__Private_User,
                relationship_weak__Private_User__has__Private_Resource,
                relationship_weak__Private_Resource__resides_on__Host,
                relationship_weak__Host__has__Network_Interface
            WHERE
                entity__BICLSU_User.Id = ?
                AND
                entity_weak__Private_Resource.Id = ?
                AND
                entity__BICLSU_User.Id = relationship__BICLSU_User__maps_to__Private_User.biclsu_user_Id
                AND
                relationship__BICLSU_User__maps_to__Private_User.private_user_host_Id = entity_weak__Private_User.host_Id
                AND
                relationship__BICLSU_User__maps_to__Private_User.private_user_Id = entity_weak__Private_User.Id
                AND
                entity_weak__Private_User.host_Id = relationship_weak__Private_User__has__Private_Resource.private_user_host_Id
                AND
                entity_weak__Private_User.Id = relationship_weak__Private_User__has__Private_Resource.private_user_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_host_Id = entity_weak__Private_Resource.host_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_private_user_Id = entity_weak__Private_Resource.private_user_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_Id = entity_weak__Private_Resource.Id
                AND
                entity_weak__Private_Resource.host_Id = relationship_weak__Private_Resource__resides_on__Host.private_resource_host_Id
                AND
                entity_weak__Private_Resource.private_user_Id = relationship_weak__Private_Resource__resides_on__Host.private_resource_private_user_Id
                AND
                entity_weak__Private_Resource.Id = relationship_weak__Private_Resource__resides_on__Host.private_resource_Id
                AND
                relationship_weak__Private_Resource__resides_on__Host.host_Id = entity__Host.Id
                AND
                entity__Host.Id = relationship_weak__Host__has__Network_Interface.host_Id
                AND
                relationship_weak__Host__has__Network_Interface.network_interface_host_Id = entity_weak__Network_Interface.host_Id
                AND
                relationship_weak__Host__has__Network_Interface.network_interface_Id = entity_weak__Network_Interface.Id
                
        """
        
        result_list = \
            self.db_operator.query_single(
                query, 
                (str(user_Id), str(resource_Id))
            )
        
        if result_list:
            network_interface_list = []
            for one_record in result_list:
                one_network_interface = {}
                one_network_interface["network"] = one_record[0]
                one_network_interface["IPv4 interface"] = one_record[1]
                one_network_interface["IPv6 interface"] = one_record[2]
                one_network_interface["capacity"] = one_record[3]
                network_interface_list.append(one_network_interface)
                #print(str(one_network_interface))
                
            one_path_information["network interface"] = network_interface_list
        
        #print("one_path_information: " + str(one_path_information))
            
        # retrieve private resource path
        query = """
            
            SELECT
                entity_weak__Private_Resource.root_absolute_path
            FROM
                entity__BICLSU_user,
                entity_weak__Private_User,
                entity_weak__Private_Resource,
                relationship__BICLSU_User__maps_to__Private_User,
                relationship_weak__Private_User__has__Private_Resource
            WHERE
                entity__BICLSU_User.Id = ?
                AND
                entity_weak__Private_Resource.Id = ?
                AND
                entity__BICLSU_User.Id = relationship__BICLSU_User__maps_to__Private_User.biclsu_user_Id
                AND
                relationship__BICLSU_User__maps_to__Private_User.private_user_host_Id = entity_weak__Private_User.host_Id
                AND
                relationship__BICLSU_User__maps_to__Private_User.private_user_Id = entity_weak__Private_User.Id
                AND
                entity_weak__Private_User.host_Id = relationship_weak__Private_User__has__Private_Resource.private_user_host_Id
                AND
                entity_weak__Private_User.Id = relationship_weak__Private_User__has__Private_Resource.private_user_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_host_Id = entity_weak__Private_Resource.host_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_Id = entity_weak__Private_Resource.Id
        """
        
        result_list = \
            self.db_operator.query_single(
                query, 
                (str(user_Id), str(resource_Id))
            )
        
        if result_list:
            one_path_information["root path"] = result_list[0][0]
            
        #print("one_path_information: " + str(one_path_information))
            
        # retrieve credential of private user
        query = """
            
            SELECT
                entity__Control_Credential.category,
                entity__Control_Credential.path,
                entity__Control_Credential.passphrase
            FROM
                entity__BICLSU_user,
                entity_weak__Private_User,
                entity__Control_Credential,
                entity_weak__Private_Resource,
                relationship__BICLSU_User__maps_to__Private_User,
                relationship__Private_User__accepts__Control_Credential,
                relationship_weak__Private_User__has__Private_Resource
            WHERE
                entity__BICLSU_User.Id = ?
                AND
                entity_weak__Private_Resource.Id = ?
                AND
                entity__BICLSU_User.Id = relationship__BICLSU_User__maps_to__Private_User.biclsu_user_Id
                AND
                relationship__BICLSU_User__maps_to__Private_User.private_user_host_Id = entity_weak__Private_User.host_Id
                AND
                relationship__BICLSU_User__maps_to__Private_User.private_user_Id = entity_weak__Private_User.Id
                AND
                entity_weak__Private_User.host_Id = relationship__Private_User__accepts__Control_Credential.private_user_host_Id
                AND
                entity_weak__Private_User.Id = relationship__Private_User__accepts__Control_Credential.private_user_Id
                AND
                relationship__Private_User__accepts__Control_Credential.control_credential_Id = entity__Control_Credential.Id
                AND
                entity_weak__Private_User.host_Id = relationship_weak__Private_User__has__Private_Resource.private_user_host_Id
                AND
                entity_weak__Private_User.Id = relationship_weak__Private_User__has__Private_Resource.private_user_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_host_Id = entity_weak__Private_Resource.host_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_Id = entity_weak__Private_Resource.Id
        """
        
        result_list = \
            self.db_operator.query_single(
                query, 
                (str(user_Id), str(resource_Id))
            )
        
        if result_list:
            control_credential_list = []
            for one_record in result_list:
                one_control_credential = {}
                one_control_credential["category"] = one_record[0]
                one_control_credential["path"] = one_record[1]
                one_control_credential["passphrase"] = one_record[2]
                control_credential_list.append(one_control_credential)
                #print("1 ctl credential: " + str(one_control_credential))
                
            one_path_information["control credential"] = control_credential_list
            
        #print("one_path_information: " + str(one_path_information))
           
        # retrieve shell info on private resource
        query = """
            
            SELECT
                entity__Service.server_type,
                entity__Service.server_version
            FROM
                entity__BICLSU_user,
                entity_weak__Private_User,
                entity_weak__Private_Resource,
                entity__Service,
                relationship__BICLSU_User__maps_to__Private_User,
                relationship_weak__Private_User__has__Private_Resource,
                gerund__Host__provides__Service,
                relationship__Private_User__uses_Shell__Host__provides__Service
            WHERE
                entity__BICLSU_User.Id = ?
                AND
                entity_weak__Private_Resource.Id = ?
                AND
                entity__BICLSU_User.Id = relationship__BICLSU_User__maps_to__Private_User.biclsu_user_Id
                AND
                relationship__BICLSU_User__maps_to__Private_User.private_user_host_Id = entity_weak__Private_User.host_Id
                AND
                relationship__BICLSU_User__maps_to__Private_User.private_user_Id = entity_weak__Private_User.Id
                AND
                entity_weak__Private_User.host_Id = relationship_weak__Private_User__has__Private_Resource.private_user_host_Id
                AND
                entity_weak__Private_User.Id = relationship_weak__Private_User__has__Private_Resource.private_user_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_host_Id = entity_weak__Private_Resource.host_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_Id = entity_weak__Private_Resource.Id
                AND
                entity_weak__Private_User.host_Id = relationship__Private_User__uses_Shell__Host__provides__Service.private_user_host_Id
                AND
                entity_weak__Private_User.Id = relationship__Private_User__uses_Shell__Host__provides__Service.private_user_Id
                AND
                relationship__Private_User__uses_Shell__Host__provides__Service.host_provides_service_host_Id = gerund__Host__provides__Service.host_Id
                AND                relationship__Private_User__uses_Shell__Host__provides__Service.host_provides_service_service_Id = gerund__Host__provides__Service.service_Id
                AND
                gerund__Host__provides__Service.service_Id = entity__Service.Id
        """
        
        result_list = \
            self.db_operator.query_single(
                query, 
                (str(user_Id), str(resource_Id))
            )
        
        if result_list:
            one_path_information["shell type"] = result_list[0][0]
            one_path_information["shell version"] = result_list[0][1]
            
        #print("one_path_information: " + str(one_path_information))
        
        # retrieve transfer application info on private resource
        query = """
        
            SELECT
                entity__Transfer_Application.priority,
                entity__Transfer_Application.display_name,
                 relationship__Private_Resource__supports__Transfer_Application.transfer_application_absolute_path,
                relationship__Private_Resource__supports__Transfer_Application.optimal_read_parallelism,
                relationship__Private_Resource__supports__Transfer_Application.optimal_read_other_parameters,
                relationship__Private_Resource__supports__Transfer_Application.optimal_write_parallelism,
                relationship__Private_Resource__supports__Transfer_Application.optimal_write_other_parameters,
                relationship__Private_Resource__supports__Transfer_Application.accept_connection
            FROM
                entity__BICLSU_user,
                entity_weak__Private_User,
                entity_weak__Private_Resource,
                entity__Transfer_Application,
                relationship__BICLSU_User__maps_to__Private_User,
                relationship_weak__Private_User__has__Private_Resource,
                relationship__Private_Resource__supports__Transfer_Application
            WHERE
                entity__BICLSU_User.Id = ?
                AND
                entity_weak__Private_Resource.Id = ?
                AND
                entity__BICLSU_User.Id = relationship__BICLSU_User__maps_to__Private_User.biclsu_user_Id
                AND
                relationship__BICLSU_User__maps_to__Private_User.private_user_host_Id = entity_weak__Private_User.host_Id
                AND
                relationship__BICLSU_User__maps_to__Private_User.private_user_Id = entity_weak__Private_User.Id
                AND
                entity_weak__Private_User.host_Id = relationship_weak__Private_User__has__Private_Resource.private_user_host_Id
                AND
                entity_weak__Private_User.Id = relationship_weak__Private_User__has__Private_Resource.private_user_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_host_Id = entity_weak__Private_Resource.host_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_Id = entity_weak__Private_Resource.Id
                AND
                entity_weak__Private_Resource.host_Id = relationship__Private_Resource__supports__Transfer_Application.private_resource_host_Id
                AND
                entity_weak__Private_Resource.private_user_Id = relationship__Private_Resource__supports__Transfer_Application.private_resource_private_user_Id
                AND
                entity_weak__Private_Resource.Id = relationship__Private_Resource__supports__Transfer_Application.private_resource_Id
                AND
                relationship__Private_Resource__supports__Transfer_Application.transfer_application_Id = entity__Transfer_Application.Id
            ORDER BY
                entity__Transfer_Application.priority
                ASC
        """
        
        result_list = \
            self.db_operator.query_single(
                query, 
                (str(user_Id), str(resource_Id))
            )
        
        if result_list:
            one_path_information["transfer application"] = []
            for one_record in result_list:
                one_application = {}
                one_application["priority"] = one_record[0]
                one_application["name"] = one_record[1]
                one_application["path"] = one_record[2]
                one_application["read parallelism"] = one_record[3]
                one_application["read parameters"] = one_record[4]
                one_application["write parallelism"] = one_record[5]
                one_application["write parameters"] = one_record[6]
                if one_record[7] == 0:
                    one_application["server"] = False
                elif one_record[7] == 1:
                    one_application["server"] = True
                one_path_information["transfer application"].append(one_application)
            
        #print("one_path_information: " + str(one_path_information))
            
        # retrieve all services and coresponding network interfaces
        query = """
        
            SELECT
                entity__Service.category,
                entity__Service.server_type,
                entity__Service.server_version,
                entity_weak__Network_Interface.network_type,
                entity_weak__Network_Interface.IPv4_or_FQDN,
                entity_weak__Network_Interface.IPv6_or_FQDN,
                entity_weak__Network_Interface.capacity,
                entity_weak__Control_Port.number
            FROM
                entity__BICLSU_user,
                entity_weak__Private_User,
                entity_weak__Private_Resource,
                entity__Host,
                entity__Service,
                entity_weak__Network_Interface,
                entity_weak__Control_Port,
                relationship__BICLSU_User__maps_to__Private_User,
                relationship_weak__Private_User__has__Private_Resource,
                relationship_weak__Private_Resource__resides_on__Host,
                gerund__Host__provides__Service,
                relationship__Host__provides__Service__listens_at__Network_Interface,
                relationship__Host__provides__Service__listens_at__Control_Port
            WHERE
                entity__BICLSU_User.Id = ?
                AND
                entity_weak__Private_Resource.Id = ?
                AND
                entity__BICLSU_User.Id = relationship__BICLSU_User__maps_to__Private_User.biclsu_user_Id
                AND
                relationship__BICLSU_User__maps_to__Private_User.private_user_host_Id = entity_weak__Private_User.host_Id
                AND
                relationship__BICLSU_User__maps_to__Private_User.private_user_Id = entity_weak__Private_User.Id
                AND
                entity_weak__Private_User.host_Id = relationship_weak__Private_User__has__Private_Resource.private_user_host_Id
                AND
                entity_weak__Private_User.Id = relationship_weak__Private_User__has__Private_Resource.private_user_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_host_Id = entity_weak__Private_Resource.host_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_private_user_Id = entity_weak__Private_Resource.private_user_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_Id = entity_weak__Private_Resource.Id
                AND
                entity_weak__Private_Resource.host_Id = relationship_weak__Private_Resource__resides_on__Host.private_resource_host_Id
                AND
                entity_weak__Private_Resource.private_user_Id = relationship_weak__Private_Resource__resides_on__Host.private_resource_private_user_Id
                AND
                entity_weak__Private_Resource.Id = relationship_weak__Private_Resource__resides_on__Host.private_resource_Id
                AND
                relationship_weak__Private_Resource__resides_on__Host.host_Id = entity__Host.Id
                AND
                entity__Service.Id = gerund__Host__provides__Service.service_Id
                AND
                gerund__Host__provides__Service.host_Id = entity__Host.Id
                AND
                gerund__Host__provides__Service.host_Id = relationship__Host__provides__Service__listens_at__Network_Interface.host_provides_service_host_Id
                AND 
                gerund__Host__provides__Service.service_Id = relationship__Host__provides__Service__listens_at__Network_Interface.host_provides_service_service_Id
                AND
relationship__Host__provides__Service__listens_at__Network_Interface.network_interface_host_Id = entity_weak__Network_Interface.host_Id
                AND
                relationship__Host__provides__Service__listens_at__Network_Interface.network_interface_Id = entity_weak__Network_Interface.Id
                AND 
                gerund__Host__provides__Service.host_Id = relationship__Host__provides__Service__listens_at__Control_Port.host_provides_service_host_Id
                AND 
                gerund__Host__provides__Service.service_Id = relationship__Host__provides__Service__listens_at__Control_Port.host_provides_service_service_Id
                AND
relationship__Host__provides__Service__listens_at__Control_Port.control_port_host_Id = entity_weak__Control_Port.host_Id
                AND
                relationship__Host__provides__Service__listens_at__Control_Port.control_port_number = entity_weak__Control_Port.number
            
            UNION
            
            SELECT
                entity__Service.category,
                entity__Service.server_type,
                entity__Service.server_version,
                Null AS network_type,
                Null AS IPv4_or_FQDN,
                Null AS IPv6_or_FQDN,
                Null AS capacity,
                Null AS number
            FROM
                entity__BICLSU_user,
                entity_weak__Private_User,
                entity_weak__Private_Resource,
                entity__Host,
                entity__Service,
                entity_weak__Network_Interface,
                entity_weak__Control_Port,
                relationship__BICLSU_User__maps_to__Private_User,
                relationship_weak__Private_User__has__Private_Resource,
                relationship_weak__Private_Resource__resides_on__Host,
                gerund__Host__provides__Service,
                relationship__Host__provides__Service__listens_at__Network_Interface
            WHERE
                entity__BICLSU_User.Id = ?
                AND
                entity_weak__Private_Resource.Id = ?
                AND
                entity__BICLSU_User.Id = relationship__BICLSU_User__maps_to__Private_User.biclsu_user_Id
                AND
                relationship__BICLSU_User__maps_to__Private_User.private_user_host_Id = entity_weak__Private_User.host_Id
                AND
                relationship__BICLSU_User__maps_to__Private_User.private_user_Id = entity_weak__Private_User.Id
                AND
                entity_weak__Private_User.host_Id = relationship_weak__Private_User__has__Private_Resource.private_user_host_Id
                AND
                entity_weak__Private_User.Id = relationship_weak__Private_User__has__Private_Resource.private_user_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_host_Id = entity_weak__Private_Resource.host_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_private_user_Id = entity_weak__Private_Resource.private_user_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_Id = entity_weak__Private_Resource.Id
                AND
                entity_weak__Private_Resource.host_Id = relationship_weak__Private_Resource__resides_on__Host.private_resource_host_Id
                AND
                entity_weak__Private_Resource.private_user_Id = relationship_weak__Private_Resource__resides_on__Host.private_resource_private_user_Id
                AND
                entity_weak__Private_Resource.Id = relationship_weak__Private_Resource__resides_on__Host.private_resource_Id
                AND
                relationship_weak__Private_Resource__resides_on__Host.host_Id = entity__Host.Id
                AND
                entity__Service.Id = gerund__Host__provides__Service.service_Id
                AND
                gerund__Host__provides__Service.host_Id = entity__Host.Id
                AND
                NOT EXISTS (
                    SELECT 1
                    FROM
                        relationship__Host__provides__Service__listens_at__Network_Interface
                    WHERE relationship__Host__provides__Service__listens_at__Network_Interface.host_provides_service_host_Id = gerund__Host__provides__Service.host_Id
                        AND relationship__Host__provides__Service__listens_at__Network_Interface.host_provides_service_service_Id = gerund__Host__provides__Service.service_Id
                    )
                    
        """
        
        result_list = \
            self.db_operator.query_single(
                query, 
                (str(user_Id), str(resource_Id), 
                str(user_Id), str(resource_Id))
            )
        
        if not result_list:
            return None
        else:
        
            #print("result: " + str(result_list))
        
            one_path_information["service"] = {}
            break_point_index_interface_dictionary = \
                len(result_list[0]) - \
                total_pairs_in_final_interface_list_element
            #print(str(break_point_index_service_dictionary))
            for one_record in result_list:
                current_dictionary = one_path_information["service"]
                for index in \
                    range(0, break_point_index_interface_dictionary, 1):
                    #print("candidate key: " + str(one_record[index]))
                    if one_record[index] not in current_dictionary:
                        if index == \
                            (break_point_index_interface_dictionary - 1):
                            current_dictionary[one_record[index]] = []
                        else:
                            #print("create key: " + str(one_record[index]))
                            current_dictionary[one_record[index]] = {}
                    if index == (break_point_index_interface_dictionary - 1): 
                        #move to final service list
                        interface_list = \
                            current_dictionary[one_record[index]]
                    else:
                        #move to next level dictionary
                        current_dictionary = \
                            current_dictionary[one_record[index]]
                        
                interface_list.append(
                    {
                        "network type": one_record[index + 1],
                        "IPv4 interface": one_record[index + 2],
                        "IPv6 interface": one_record[index + 3],
                        "capacity": one_record[index + 4],
                        "port": one_record[index + 5]
                    }
                )

        #print("path info: " + str(one_path_information))
        
        return one_path_information
    
    # end of function getPartnerSystemInformation
    
    def mapLogicalPathsToDisplayNames(self, logical_path_segments):
        
        total_segments = len(logical_path_segments)
        
        return_value = []
        
        # TO DO: add autonomous region support
        return_value.append(
            {"logical": "/0/", 
            "display": "LSU Baton Rouge"}
            )
        
        if total_segments >= 2:
        
            query = """
            
                SELECT
                    entity__BICLSU_User.display_name
                FROM
                    entity__BICLSU_User
                WHERE
                    entity__BICLSU_User.Id = ?
            """
            
            result_list = \
                self.db_operator.query_single(
                    query, 
                    (int(logical_path_segments[1]),)
                )
            
            if not result_list:
                return None
                
            return_value.append(
                {"logical": str(logical_path_segments[1]) + '/', 
                "display": str(result_list[0][0])}
                )
           
        if total_segments >= 3:
           
            query = """
                
                SELECT
                    entity_weak__Private_Resource.display_name
                FROM
                    entity_weak__Private_Resource
                WHERE
                    entity_weak__Private_Resource.Id = ?
            
            """
        
            result_list = \
                self.db_operator.query_single(
                    query, 
                    (int(logical_path_segments[2]),)
                )
            
            if not result_list:
                return None
                
            return_value.append(
                {"logical": str(logical_path_segments[2]) + '/', 
                "display": str(result_list[0][0])}
                )
        
        #print("log to disp: " + str(return_value))
        
        return return_value
    
    # end of function mapLogicalPathsToDisplayNames
    
    def getPrivateUsers(self, private_resources):
    
        query = """
        
            SELECT
                entity_weak__Private_User.Id,
                entity__Service.server_type
            FROM
                entity_weak__Private_User,
                entity_weak__Private_Resource,
                entity__Service,
                gerund__Host__provides__Service,
                relationship__Private_Resource__belongs_to__Host__provides__Service,
                relationship_weak__Private_User__has__Private_Resource
            WHERE
                entity_weak__Private_Resource.Id
                    IN ({})
                AND
                entity_weak__Private_User.host_Id = relationship_weak__Private_User__has__Private_Resource.private_user_host_Id
                AND
                entity_weak__Private_User.Id = relationship_weak__Private_User__has__Private_Resource.private_user_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_host_Id = entity_weak__Private_Resource.host_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_private_user_Id = entity_weak__Private_Resource.private_user_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_Id = entity_weak__Private_Resource.Id
                AND
                entity_weak__Private_Resource.host_Id = relationship__Private_Resource__belongs_to__Host__provides__Service.private_resource_host_Id
                AND
                entity_weak__Private_Resource.private_user_Id = relationship__Private_Resource__belongs_to__Host__provides__Service.private_resource_private_user_Id
                AND
                entity_weak__Private_Resource.Id = relationship__Private_Resource__belongs_to__Host__provides__Service.private_resource_Id
                AND
                relationship__Private_Resource__belongs_to__Host__provides__Service.host_provides_service_host_Id = gerund__Host__provides__Service.host_Id
                AND
                relationship__Private_Resource__belongs_to__Host__provides__Service.host_provides_service_service_Id = gerund__Host__provides__Service.service_Id
                AND
                gerund__Host__provides__Service.service_Id = entity__Service.Id
     
        """.format(','.join('?' for i in private_resources)) 
        
        query_new = """
        
            SELECT
                entity_weak__Private_User.Id
            FROM
                entity_weak__Private_User,
                entity_weak__Private_Resource,
                relationship_weak__Private_User__has__Private_Resource
            WHERE
                entity_weak__Private_Resource.Id
                    IN ({})
                AND
                entity_weak__Private_User.host_Id = relationship_weak__Private_User__has__Private_Resource.private_user_host_Id
                AND
                entity_weak__Private_User.Id = relationship_weak__Private_User__has__Private_Resource.private_user_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_host_Id = entity_weak__Private_Resource.host_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_private_user_Id = entity_weak__Private_Resource.private_user_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_Id = entity_weak__Private_Resource.Id
                
     
        """.format(','.join('?' for i in private_resources))
        
        result_list = \
            self.db_operator.query_single(
                query, 
                private_resources
            )
  
        #print("private user,resource: " + str(result_list))
        
        return_value = []
        for one_record in result_list:
            return_value.append(
                {"private user Id": one_record[0],
                "server type": one_record[1]})
    
        return return_value
    
    # end of function getPrivateUser
    
    def getStorageServicesOf(self, private_resources):
    
        query = """
        
            SELECT
                entity__Service.server_type
            FROM
                entity_weak__Private_Resource,
                entity__Service,
                gerund__Host__provides__Service,
                relationship__Private_Resource__belongs_to__Host__provides__Service
            WHERE
                entity_weak__Private_Resource.Id
                    IN ({})
                AND
                entity_weak__Private_Resource.host_Id = relationship__Private_Resource__belongs_to__Host__provides__Service.private_resource_host_Id
                AND
                entity_weak__Private_Resource.private_user_Id = relationship__Private_Resource__belongs_to__Host__provides__Service.private_resource_private_user_Id
                AND
                entity_weak__Private_Resource.Id = relationship__Private_Resource__belongs_to__Host__provides__Service.private_resource_Id
                AND
                relationship__Private_Resource__belongs_to__Host__provides__Service.host_provides_service_host_Id = gerund__Host__provides__Service.host_Id
                AND
                relationship__Private_Resource__belongs_to__Host__provides__Service.host_provides_service_service_Id = gerund__Host__provides__Service.service_Id
                AND
                gerund__Host__provides__Service.service_Id = entity__Service.Id
                AND
                entity__Service.category = 'storage'
                
        """.format(','.join('?' for i in private_resources))
        
        result_list = \
            self.db_operator.query_single(
                query, 
                private_resources
            )
  
        #print("storage: " + str(result_list))
    
        return_value = []
        for one_record in result_list:
            return_value.append(one_record[0])
        
        return return_value
    
    # end of function getStorageServices
    
    def get3rdPartyTransmissionControlInformation(self):
        
        query = """
        
            SELECT
                entity__BICLSU_User.Id,
                entity_weak__Private_Resource.Id
            FROM
                entity__BICLSU_User,
                entity_weak__Private_User,
                entity_weak__Private_Resource,
                relationship__BICLSU_User__maps_to__Private_User,
                relationship_weak__Private_User__has__Private_Resource
            WHERE
                entity__BICLSU_User.display_name IN ('BIC-LSU Transmission Admin', 'Test Transmission Admin')
                AND
                entity__BICLSU_User.Id = relationship__BICLSU_User__maps_to__Private_User.biclsu_user_Id
                AND
                relationship__BICLSU_User__maps_to__Private_User.private_user_host_Id = entity_weak__Private_User.host_Id
                AND
                relationship__BICLSU_User__maps_to__Private_User.private_user_Id = entity_weak__Private_User.Id
                AND
                entity_weak__Private_User.host_Id = relationship_weak__Private_User__has__Private_Resource.private_user_host_Id
                AND
                entity_weak__Private_User.Id = relationship_weak__Private_User__has__Private_Resource.private_user_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_host_Id = entity_weak__Private_Resource.host_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_private_user_Id = entity_weak__Private_Resource.private_user_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_Id = entity_weak__Private_Resource.Id
                
        """
        
        result_list = \
            self.db_operator.query_single(
                query)
  
        #print("trans ctl resc: " + str(result_list))
        
        return result_list
    
    # end of function get3rdPartyTransmissionControlInformation
    
    def getBICLSUiRODSAdminInformation(self, BICLSU_private_resouce):
    
        query = """
        
            SELECT
                entity__BICLSU_User.Id,
                entity_weak__Private_Resource.Id
            FROM
                entity__BICLSU_User,
                entity_weak__Private_User,
                entity_weak__Private_Resource,
                relationship__BICLSU_User__maps_to__Private_User,
                relationship_weak__Private_User__has__Private_Resource
            WHERE
                entity__BICLSU_User.display_name IN ('BIC-LSU iRODS Admin','Test iRODS Admin')
                AND
                entity__BICLSU_User.Id = relationship__BICLSU_User__maps_to__Private_User.biclsu_user_Id
                AND
                relationship__BICLSU_User__maps_to__Private_User.private_user_host_Id = entity_weak__Private_User.host_Id
                AND
                relationship__BICLSU_User__maps_to__Private_User.private_user_Id = entity_weak__Private_User.Id
                AND
                entity_weak__Private_User.host_Id = relationship_weak__Private_User__has__Private_Resource.private_user_host_Id
                AND
                entity_weak__Private_User.Id = relationship_weak__Private_User__has__Private_Resource.private_user_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_host_Id = entity_weak__Private_Resource.host_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_private_user_Id = entity_weak__Private_Resource.private_user_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_Id = entity_weak__Private_Resource.Id
                AND
                entity_weak__Private_Resource.host_Id = 
                    (SELECT
                        entity_weak__Private_Resource.host_Id
                    FROM
                        entity_weak__Private_Resource
                    WHERE
                        entity_weak__Private_Resource.Id = ?)
                
        """    
        
        result_list = \
            self.db_operator.query_single(
                query,
                (BICLSU_private_resouce,))
  
        #print("biclsu irods admin resc: " + str(result_list))
        
        return result_list
    
    # end of function getBICLSUiRODSAdminInformation
    
    def getTransferApplicationOf(self, private_resource):
    
        pass
    
    # end of function getTransferApplicationOf
    
    def reserveDataPorts(self, private_resource, total):
    
        query_find = """
        
            SELECT
                entity_weak__Data_Port.number
            FROM
                entity__Host,
                entity_weak__Private_Resource,
                entity_weak__Data_Port,
                relationship_weak__Private_Resource__resides_on__Host,
                relationship_weak__Host__has__Data_Port
            WHERE
                entity_weak__Private_Resource.Id = ?
                AND
                entity_weak__Data_Port.in_use = '0'
                AND
                entity_weak__Private_Resource.host_Id = relationship_weak__Private_Resource__resides_on__Host.private_resource_host_Id
                AND
                entity_weak__Private_Resource.private_user_Id = relationship_weak__Private_Resource__resides_on__Host.private_resource_private_user_Id
                AND
                entity_weak__Private_Resource.Id = relationship_weak__Private_Resource__resides_on__Host.private_resource_Id
                AND
                relationship_weak__Private_Resource__resides_on__Host.host_Id = entity__Host.Id
                AND
                entity__Host.Id = relationship_weak__Host__has__Data_Port.host_Id
                AND
                relationship_weak__Host__has__Data_Port.data_port_host_Id = entity_weak__Data_Port.host_Id
                AND
                relationship_weak__Host__has__Data_Port.data_port_number = entity_weak__Data_Port.number
                
        """
        
        query_reserve = """
        
            UPDATE
                entity_weak__Data_Port
            SET
                in_use = '1'
            WHERE
                in_use = '0'
                AND
                number in ({})
                AND
                EXISTS (
                    SELECT 1
                    FROM
                        entity__Host,
                        entity_weak__Private_Resource,
                        entity_weak__Data_Port,
                        relationship_weak__Private_Resource__resides_on__Host,
                        relationship_weak__Host__has__Data_Port
                    WHERE
                        entity_weak__Private_Resource.Id = ?
                        AND
                        entity_weak__Private_Resource.host_Id = relationship_weak__Private_Resource__resides_on__Host.private_resource_host_Id
                        AND
                        entity_weak__Private_Resource.private_user_Id = relationship_weak__Private_Resource__resides_on__Host.private_resource_private_user_Id
                        AND
                        entity_weak__Private_Resource.Id = relationship_weak__Private_Resource__resides_on__Host.private_resource_Id
                        AND
                        relationship_weak__Private_Resource__resides_on__Host.host_Id = entity__Host.Id
                        AND
                        entity__Host.Id = relationship_weak__Host__has__Data_Port.host_Id
                        AND
                        relationship_weak__Host__has__Data_Port.data_port_host_Id = entity_weak__Data_Port.host_Id
                        AND
                        relationship_weak__Host__has__Data_Port.data_port_number = entity_weak__Data_Port.number
                )
            
        """
    
        try:
            self.db_operator.transaction_begin()
            
            # find
            result_list = \
                self.db_operator.query_single(
                    query_find,
                    private_resource)
                    
            if not result_list or total > len(result_list):
                raise Exception("Cannot find sufficient available ports.")
                
            #print("result list: " + str(result_list))
                
            port_list = []
            for index in range(0, total, 1):
                port_list.append(result_list[index][0])
                
            #print("port list: " + str(port_list))
     
            # reserve
            binding_list = list(port_list)
            binding_list.append(private_resource)
            
            #print("bindings: " + str(binding_list))
            
            self.db_operator.query_single(
                query_reserve.format(','.join('?' for i in range(0, total, 1))),
                binding_list)
            
            # for confirming the ports are reserved    
            #result_list = \
            #   self.db_operator.query_single(
            #        query_find,
            #        private_resource)
            #print("result: " + str(result_list))
            
            self.db_operator.transaction_commit()
        except Exception as e:
            self.db_operator.transaction_rollback()
            port_list = []
            print("Exception while reserving data ports: " + str(e.message))
            
        return port_list
    
    # end of function reserveDataPorts
    
    def releaseDataPorts(self, private_resource, reserved_ports):

        query_release = """
        
            UPDATE
                entity_weak__Data_Port
            SET
                in_use = '0'
            WHERE
                in_use = '1'
                AND
                number in ({})
                AND
                EXISTS (
                    SELECT 1
                    FROM
                        entity__Host,
                        entity_weak__Private_Resource,
                        entity_weak__Data_Port,
                        relationship_weak__Private_Resource__resides_on__Host,
                        relationship_weak__Host__has__Data_Port
                    WHERE
                        entity_weak__Private_Resource.Id = ?
                        AND
                        entity_weak__Private_Resource.host_Id = relationship_weak__Private_Resource__resides_on__Host.private_resource_host_Id
                        AND
                        entity_weak__Private_Resource.private_user_Id = relationship_weak__Private_Resource__resides_on__Host.private_resource_private_user_Id
                        AND
                        entity_weak__Private_Resource.Id = relationship_weak__Private_Resource__resides_on__Host.private_resource_Id
                        AND
                        relationship_weak__Private_Resource__resides_on__Host.host_Id = entity__Host.Id
                        AND
                        entity__Host.Id = relationship_weak__Host__has__Data_Port.host_Id
                        AND
                        relationship_weak__Host__has__Data_Port.data_port_host_Id = entity_weak__Data_Port.host_Id
                        AND
                        relationship_weak__Host__has__Data_Port.data_port_number = entity_weak__Data_Port.number
                )
            
        """
        
        try:
            self.db_operator.transaction_begin()
        
            # reserve
            binding_list = reserved_ports
            binding_list.append(private_resource)
            
            #print("bindings: " + str(binding_list))
            
            self.db_operator.query_single(
                query_release.format(
                    ','.join('?' for i in range(0, len(reserved_ports) -1, 1))),
                binding_list)
                
            self.db_operator.transaction_commit()

            return_value = True
        except Exception as e:
            self.db_operator.transaction_rollback()
            return_value = False
            print("Exception while reserving data ports: " + str(e.message))
            
        return return_value
            
    # end of function releaseDataPorts
    
    def listHostWithApplication(self, applications):
    
        query = """
        
            SELECT
                entity__Analytic_Application.Id,
                entity__Host.Id,
                entity__Host.display_name
            FROM
                entity__Host,
                entity__Service,
                entity__Analytic_Application,
                gerund__Host__provides__Service,
                relationship__Analytic_Application__scheduled_by__Host__provides__Service
            WHERE
                entity__Analytic_Application.Id 
                    IN ({})
                AND
                entity__Analytic_Application.Id = relationship__Analytic_Application__scheduled_by__Host__provides__Service.analytic_application_Id
                AND
                relationship__Analytic_Application__scheduled_by__Host__provides__Service.host_provides_service_host_Id = entity__Host.Id
                AND
                relationship__Analytic_Application__scheduled_by__Host__provides__Service.host_provides_service_service_Id = entity__Service.Id
                AND
                entity__Host.Id =
                    gerund__Host__provides__Service.host_Id
                AND
                gerund__Host__provides__Service.service_Id = 
                    entity__Service.Id
        
        """.format(','.join('?' for i in applications))
        
        return_value = {}
        
        result_list = \
            self.db_operator.query_single(
                    query,
                    applications
                )
                         
        if not result_list:
            return_value = None
        else:
            #print("ret ls: " + str(result_list))
            for one_host in result_list:
                if one_host[0] not in return_value:
                    return_value[one_host[0]] = []
              
                return_value[one_host[0]].append(
                    {"host Id": one_host[1],
                    "host display name": one_host[2]}
                    )
        
        #print("ret val: " + str(return_value))
        
        return return_value
    
    # end of function listHostWithServices
    
    def listHostServiceWithCategoryApplication(self, list_info_list):
        
        return_value = {}
        
        for one_info in list_info_list:
        
            # To Do
            #use complete query
            if "application" in one_info and \
                "service" in one_info:
                # list hosts
                query_string = """
                    SELECT
                        entity__Host.Id,
                        entity__Host.display_name
                    FROM
                        entity__Host,
                        entity__Analytic_Application,
                        gerund__Host__provides__Service,
                        relationship__Analytic_Application__scheduled_by__Host__provides__Service
                    WHERE
                        entity__Analytic_Application.Id = ?
                        AND
                        gerund__Host__provides__Service.service_Id = ?
                        AND
                        entity__Host.Id = gerund__Host__provides__Service.host_Id
                        AND
                        gerund__Host__provides__Service.host_Id = relationship__Analytic_Application__scheduled_by__Host__provides__Service.host_provides_service_host_Id
                        AND
                        gerund__Host__provides__Service.service_Id = relationship__Analytic_Application__scheduled_by__Host__provides__Service.host_provides_service_service_Id
                        AND
                        relationship__Analytic_Application__scheduled_by__Host__provides__Service.analytic_application_Id = entity__Analytic_Application.Id
                    """
                result = \
                    self.db_operator.query_single(
                    query_string,
                    (int(one_info["application"]), int(one_info["service"])))
                
                key = \
                    str((one_info["application"],
                        "",
                        one_info["service"]))
                        
                return_value[key] = []
                for one_result in result:
                    return_value[key].append(
                    {"host_Id": one_result[0],
                    "host_display_name": one_result[1]})
                
            elif "category" in one_info and \
                "service" in one_info:
                # list hosts
                # TO DO
                #remove this branch
                query_string = """
                    SELECT
                        entity__Host.Id,
                        entity__Host.display_name
                    FROM
                        entity__Host,
                        entity__Analytic_Application,
                        entity__Service,
                        gerund__Host__provides__Service,
                        relationship__Analytic_Application__scheduled_by__Host__provides__Service
                    WHERE
                        entity__Service.Id = ?
                        AND
                        entity__Service.category = ?
                        AND
                        entity__Service.Id = gerund__Host__provides__Service.service_Id
                        AND
                        entity__Host.Id = gerund__Host__provides__Service.host_Id
                        AND
                        gerund__Host__provides__Service.host_Id = relationship__Analytic_Application__scheduled_by__Host__provides__Service.host_provides_service_host_Id
                        AND
                        gerund__Host__provides__Service.service_Id = relationship__Analytic_Application__scheduled_by__Host__provides__Service.host_provides_service_service_Id
                        AND
                        relationship__Analytic_Application__scheduled_by__Host__provides__Service.analytic_application_Id = entity__Analytic_Application.Id
                    GROUP BY
                        entity__Host.Id,
                        entity__Host.display_name
                    """
                result = \
                    self.db_operator.query_single(
                    query_string,
                    (int(one_info["service"]), str(one_info["category"])))
                
                key = \
                    str(("",
                        one_info["category"],
                        one_info["service"]))
                        
                return_value[key] = []
                for one_result in result:
                    return_value[key].append(
                    {"host_Id": one_result[0],
                    "host_display_name": one_result[1]})
                
                
                return_value[key] = "test"
                
            elif "application" in one_info:
                # list host-service pairs
                query_string = """
                    SELECT
                        entity__Host.Id,
                        entity__Host.display_name,
                        entity__Service.Id,
                        entity__Service.server_type,
                        entity__Service.display_name
                    FROM
                        entity__Host,
                        entity__Service,
                        entity__Analytic_Application,
                        gerund__Host__provides__Service,
                        relationship__Analytic_Application__scheduled_by__Host__provides__Service
                    WHERE
                        entity__Analytic_Application.Id = ?
                        AND
                        entity__Service.Id = gerund__Host__provides__Service.service_Id
                        AND
                        entity__Host.Id = gerund__Host__provides__Service.host_Id
                        AND
                        gerund__Host__provides__Service.host_Id = relationship__Analytic_Application__scheduled_by__Host__provides__Service.host_provides_service_host_Id
                        AND
                        gerund__Host__provides__Service.service_Id = relationship__Analytic_Application__scheduled_by__Host__provides__Service.host_provides_service_service_Id
                        AND
                        relationship__Analytic_Application__scheduled_by__Host__provides__Service.analytic_application_Id = entity__Analytic_Application.Id
                    GROUP BY
                        entity__Host.Id,
                        entity__Host.display_name,
                        entity__Service.Id,
                        entity__Service.server_type,
                        entity__Service.display_name
                    """
                result = \
                    self.db_operator.query_single(
                    query_string,
                    (int(one_info["application"]),))
                
                key = \
                    str((one_info["application"],
                        "",
                        ""))
                
                return_value[key] = []
                for one_result in result:
                    return_value[key].append(
                    {"host_Id": one_result[0],
                    "host_display_name": one_result[1],
                    "service_Id": one_result[2],
                    "service_server_type": one_result[3],
                    "service_display_name": one_result[4]})
                
            elif "category" in one_info:
                # list host-service pairs
                query_string = """
                    SELECT
                        entity__Host.Id,
                        entity__Host.display_name,
                        entity__Service.category,
                        entity__Service.Id,
                        entity__Service.server_type,
                        entity__Service.display_name
                    FROM
                        entity__Host,
                        entity__Service,
                        gerund__Host__provides__Service
                    WHERE
                        entity__Service.category
                            IN ({})
                        AND
                        entity__Service.Id = gerund__Host__provides__Service.service_Id
                        AND
                        entity__Host.Id = gerund__Host__provides__Service.host_Id
                    
                    """.format(','.join('?' for i in one_info["category"]))
                result = \
                    self.db_operator.query_single(
                    query_string,
                    one_info["category"])
                
                key = \
                    str(("",
                        one_info["category"],
                        ""))
                
                return_value[key] = []
                for one_result in result:
                    return_value[key].append(
                    {"host_Id": one_result[0],
                    "host_display_name": one_result[1],
                    "service_category": one_result[2],
                    "service_Id": one_result[3],
                    "service_server_type": one_result[4],
                    "service_display_name": one_result[5]})
                
            else:
                # list host-service-application tuples
                key = \
                    str(("",
                        "",
                        ""))
                return_value[key] = "test"
                
        return return_value
        
    # end of function listHostServiceWithCategoryApplication
    
    def getPrivateStorageResourcesOf(self, BICLSU_user_Ids):
    
        query = """
        
            SELECT
                entity_weak__Private_Resource.Id,
                entity_weak__Private_Resource.display_name
            FROM
                entity__BICLSU_User,
                entity_weak__Private_User,
                entity_weak__Private_Resource,
                relationship__BICLSU_User__maps_to__Private_User,
                relationship_weak__Private_User__has__Private_Resource
            WHERE
                entity__BICLSU_User.Id = ?
                AND
                entity_weak__Private_Resource.root_absolute_path <> ''
                AND
                entity__BICLSU_User.Id = relationship__BICLSU_User__maps_to__Private_User.biclsu_user_Id
                AND
                relationship__BICLSU_User__maps_to__Private_User.private_user_host_Id = entity_weak__Private_User.host_Id
                AND
                relationship__BICLSU_User__maps_to__Private_User.private_user_Id = 
                entity_weak__Private_User.Id
                AND
                entity_weak__Private_User.host_Id = relationship_weak__Private_User__has__Private_Resource.private_user_host_Id
                AND
                entity_weak__Private_User.Id = relationship_weak__Private_User__has__Private_Resource.private_user_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_host_Id = entity_weak__Private_Resource.host_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_private_user_Id = entity_weak__Private_Resource.private_user_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_Id = entity_weak__Private_Resource.Id
                
        """
        
        return_value = {}
        
        for one_Id in BICLSU_user_Ids:
       
            result_list = \
                self.db_operator.query_single(
                    query,
                    (one_Id,))
                         
            if not result_list:
                return_value[one_Id] = None
                continue
                
            return_value[one_Id] = []
            
            for one_record in result_list:
                one_resource = {}
                one_resource["private resource Id"] = one_record[0]
                one_resource["display"] = one_record[1]
                return_value[one_Id].append(one_resource)
            
        # end of for
        
        print("ret val: " + str(return_value))
        
        return return_value
    
    # end of function getPrivateStorageResourcesOf
    
    def getPrivateComputingResourcesOf(self, BICLSU_user_Ids):
    
        query = """
        
            SELECT
                entity_weak__Private_Resource.Id,
                entity_weak__Private_Resource.display_name
            FROM
                entity__BICLSU_User,
                entity_weak__Private_User,
                entity_weak__Private_Resource,
                relationship__BICLSU_User__maps_to__Private_User,
                relationship_weak__Private_User__has__Private_Resource
            WHERE
                entity__BICLSU_User.Id = ?
                AND
                entity_weak__Private_Resource.root_absolute_path = ''
                AND
                entity__BICLSU_User.Id = relationship__BICLSU_User__maps_to__Private_User.biclsu_user_Id
                AND
                relationship__BICLSU_User__maps_to__Private_User.private_user_host_Id = entity_weak__Private_User.host_Id
                AND
                relationship__BICLSU_User__maps_to__Private_User.private_user_Id = 
                entity_weak__Private_User.Id
                AND
                entity_weak__Private_User.host_Id = relationship_weak__Private_User__has__Private_Resource.private_user_host_Id
                AND
                entity_weak__Private_User.Id = relationship_weak__Private_User__has__Private_Resource.private_user_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_host_Id = entity_weak__Private_Resource.host_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_private_user_Id = entity_weak__Private_Resource.private_user_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_Id = entity_weak__Private_Resource.Id
                
        """
        
        return_value = {}
        
        for one_Id in BICLSU_user_Ids:
       
            result_list = \
                self.db_operator.query_single(
                    query,
                    (one_Id,))
                         
            if not result_list:
                return_value[one_Id] = None
                continue
                
            return_value[one_Id] = []
            
            for one_record in result_list:
                one_resource = {}
                one_resource["private resource Id"] = one_record[0]
                one_resource["display"] = one_record[1]
                return_value[one_Id].append(one_resource)
            
        # end of for
        
        return return_value
    
    # end of function getPrivateComputingResourcesOf
    
    def getReadableEntriesAtOtherResourcesOf(self, biclsu_user_Id):
    
        pass
        
    # end of function 
    
    ##
    # @brief                Add new application and computing facility 
    #                       combinations.
    #
    # @param    parameter   A list of tuples.
    #                       [(<display name>,<host>,<service>, 
    #                       <tagged script path>,<GUI skeleton>,
    #                       <converted script path>), ...]
    def addNewApplication(self, application_list):
    
        for one_application in application_list:
            #print("app info: " + str(one_application))
            # add application
            query_string = """
                INSERT INTO
                    entity__Analytic_Application
                    (
                        display_name
                    )
                    VALUES (?)
                """
            self.db_operator.query_single(
                query_string,
                (str(one_application[0]),)) 

            # associate with scheduling service                
            query_string = """
                SELECT
                    entity__Analytic_Application.Id
                FROM
                    entity__Analytic_Application
                WHERE
                    entity__Analytic_Application.display_name = ?
            
                """
            result = \
                self.db_operator.query_single(
                    query_string,
                    (str(one_application[0]),)) 
            application_Id = result[0][0]
            #print("app Id: " + str(application_Id))
                
            query_string = """
                INSERT INTO
                    relationship__Analytic_Application__scheduled_by__Host__provides__Service
                    (
                        analytic_application_Id,
                        host_provides_service_host_Id,
                        host_provides_service_service_Id,
                        tagged_script_path,
                        GUI_skeleton,
                        converted_script_path
                    )
                    VALUES (?,?,?,?,?,?)
                """
            
            self.db_operator.query_single(
                query_string, 
                (int(application_Id), int(one_application[1]), 
                int(one_application[2]), 
                one_application[3], str(one_application[4]), 
                one_application[5]))
        # end of for
        
        #query_string = """
        #        SELECT
        #            analytic_application_Id,
        #            host_provides_service_host_Id,
        #            host_provides_service_service_Id,
        #            tagged_script_path,
        #            converted_script_path
        #        FROM
        #            relationship__Analytic_Application__scheduled_by__Host__provides__Service
        #        """
            
        #result = \
        #    self.db_operator.query_single(query_string)
        #print("app_sched_by_service: " + str(result))
    
    # end of function addNewApplication
    
    ##
    # @brief                Add computing facility for existing application.
    #
    # @param    parameter   A list of tuples.
    #                       [(<application>,<host>,<service>, 
    #                       <tagged script path>,<GUI skeleton>,
    #                       <converted script path>), ...]
    def addComputingFacilityForApplication(self, application_info_list):
    
        # associate with scheduling service
        query_string = """
        
            INSERT INTO
                relationship__Analytic_Application__scheduled_by__Host__provides__Service
                (
                    analytic_application_Id,
                    host_provides_service_host_Id,
                    host_provides_service_service_Id,
                    tagged_script_path,
                    GUI_skeleton,
                    converted_script_path
                )
                VALUES (?,?,?,?,?,?)
        
            """
            
        self.db_operator.query_repetitive(
            query_string, 
            application_info_list)
        
    # end of function addComputingFacilityForApplication
    
    ##
    # @brief    Retrieve all existing applications 
    #           and existing scheduling services on hosts.
    #
    # @return   A tuple of 2 lists, (<app list>, <svc host list>).
    #           <app list>: [(<app Id>,<display name>), ...].
    #           <svc host list>: [(<host Id>,<host name>,<svc Id>,<svc name>)].
    def listApplicationForAdding(self):
  
        return_value = {"application": [], 
                        "host_service": []}
  
        # retrieve application
        query_string = """
            SELECT
                entity__Analytic_Application.Id,
                entity__Analytic_Application.display_name
            FROM
                entity__Analytic_Application
            """
        result = self.db_operator.query_single(query_string)
        #print("app ls: " + str(result))
        for one_element in result:
            return_value["application"].append(
                {"Id": one_element[0],
                "display_name": one_element[1]})
                
        # retrieve scheduling service on host
        query_string = """
            SELECT
                gerund__Host__provides__Service.host_Id,
                gerund__Host__provides__Service.service_Id
            FROM
                entity__Host,
                entity__Service,
                gerund__Host__provides__Service
            WHERE
                entity__Service.category = 'scheduling'
                AND
                entity__Host.Id = gerund__Host__provides__Service.host_Id
                AND
                gerund__Host__provides__Service.service_Id = entity__Service.Id
        
            """
        result = self.db_operator.query_single(query_string)
        #print("svc host ls: " + str(result))
        for one_element in result:
            return_value["host_service"].append(
                {"host": one_element[0],
                "service": one_element[1]})
  
        #print("ret val: " + str(return_value))            
        return return_value
    
    # listApplicationForAdding
    
    ##
    # @brief    Retrieve all existing applications 
    #           and existing scheduling services on hosts for a list of users.
    #
    # @param    user_list   A list of myLSU IDs.
    #
    # @return   A list of dictionaries.
    #           [{"application_Id": <app id>, "application_name": <app name>,
    #           "host_Id": <host id>, "host_name": <host name>},
    #           "service_Id": <svc id>, "service_name": <svc name>}, ...]  
    def listApplicationForLaunching(self, user_list):
    
        return_value = {}
    
        query_string = """
            SELECT
                entity__Analytic_Application.Id,
                entity__Analytic_Application.display_name,
                entity__Host.Id,
                entity__Host.display_name,
                entity__Service.Id,
                entity__Service.display_name
            FROM
                entity__BICLSU_User,
                entity_weak__Private_User,
                entity_weak__Private_Resource,
                entity__Analytic_Application,
                entity__Host,
                entity__Service,
                relationship__BICLSU_User__maps_to__Private_User,
                relationship_weak__Private_User__has__Private_Resource,
                relationship_weak__Private_Resource__resides_on__Host,
                gerund__Host__provides__Service,
                relationship__Analytic_Application__scheduled_by__Host__provides__Service
            WHERE
                entity__BICLSU_User.myLSU_Id = ?
                AND
                entity__Service.category = "scheduling"
                AND
                entity__BICLSU_User.Id = relationship__BICLSU_User__maps_to__Private_User.biclsu_user_Id
                AND
                relationship__BICLSU_User__maps_to__Private_User.private_user_Id = entity_weak__Private_User.Id
                AND
                entity_weak__Private_User.host_Id = relationship_weak__Private_User__has__Private_Resource.private_user_host_Id
                AND
                entity_weak__Private_User.Id = relationship_weak__Private_User__has__Private_Resource.private_user_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_host_Id = entity_weak__Private_Resource.host_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_private_user_Id = entity_weak__Private_Resource.private_user_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_Id = entity_weak__Private_Resource.Id
                AND
                entity_weak__Private_Resource.host_Id = relationship_weak__Private_Resource__resides_on__Host.private_resource_host_Id
                AND
                entity_weak__Private_Resource.private_user_Id = relationship_weak__Private_Resource__resides_on__Host.private_resource_private_user_Id
                AND
                entity_weak__Private_Resource.Id = relationship_weak__Private_Resource__resides_on__Host.private_resource_Id
                AND
                relationship_weak__Private_Resource__resides_on__Host.host_Id = entity__Host.Id
                AND
                entity__Host.Id = gerund__Host__provides__Service.host_Id
                AND
                gerund__Host__provides__Service.service_Id = entity__Service.Id
                AND
                gerund__Host__provides__Service.host_Id = relationship__Analytic_Application__scheduled_by__Host__provides__Service.host_provides_service_host_Id
                AND
                gerund__Host__provides__Service.service_Id = relationship__Analytic_Application__scheduled_by__Host__provides__Service.host_provides_service_service_Id
                AND
                relationship__Analytic_Application__scheduled_by__Host__provides__Service.analytic_application_Id = entity__Analytic_Application.Id
            GROUP BY
                entity__Analytic_Application.Id,
                entity__Host.Id,
                entity__Service.Id
            """
            
        for one_user in user_list:
        
            result = \
                self.db_operator.query_single(
                    query_string,
                    (str(one_user),))
            #print("app 4 run: " + str(result))
        
            return_value[one_user] = []
            for one_application in result:
                return_value[one_user].append({
                "application_Id": one_application[0],
                "application_name": one_application[1],
                "host_Id": one_application[2],
                "host_name": one_application[3],
                "service_Id": one_application[4],
                "service_name": one_application[5]})
        
        # end of for
            
        #print("ret val: " + str(return_value))            
        return return_value
    
    # end of function listApplicationForLaunching
    
    ##
    # @brief    Retrieve tagged job script paths for applications.
    #
    # @param    application_info_list   A list of tuples.
    #                                   [(<application>,<host>,<service>), ...]
    # @return   A dictionary.
    #           {<app info>: (tagged script path, converted script path)}
    def getApplication(self, application_info_list):
    
        return_value = {}
    
        query_string = """
            SELECT
                relationship__Analytic_Application__scheduled_by__Host__provides__Service.tagged_script_path,
                relationship__Analytic_Application__scheduled_by__Host__provides__Service.converted_script_path
            FROM
                entity__Analytic_Application,
                entity__Host,
                entity__Service,
                gerund__Host__provides__Service,
                relationship__Analytic_Application__scheduled_by__Host__provides__Service
            WHERE
                entity__Analytic_Application.Id = ?
                AND
                entity__Host.Id = ?
                AND
                entity__Service.Id = ?
                AND
                entity__Host.Id = gerund__Host__provides__Service.host_Id
                AND
                gerund__Host__provides__Service.service_Id = entity__Service.Id
                AND
                entity__Analytic_Application.Id = relationship__Analytic_Application__scheduled_by__Host__provides__Service.analytic_application_Id
                AND
                relationship__Analytic_Application__scheduled_by__Host__provides__Service.host_provides_service_host_Id = gerund__Host__provides__Service.host_Id
                AND
                relationship__Analytic_Application__scheduled_by__Host__provides__Service.host_provides_service_service_Id = gerund__Host__provides__Service.service_Id
                
            """
            
        for one_application in application_info_list:
        
            result = \
                self.db_operator.query_single(
                    query_string,
                    (int(one_application[0]),
                    int(one_application[1]),
                    int(one_application[2])))
            #print("tagged script: " + str(result))
            
            key = \
                str((int(one_application[0]),
                    int(one_application[1]),
                    int(one_application[2])))
            return_value[key] = \
                {"tagged": result[0][0],
                "converted": result[0][1]}
            
        # end of for
        
        #print("ret val: " + str(return_value))
        return return_value
    
    # end of function getApplication
    
    ##
    # @brief    Retrieve GUI skeletons for applications.
    #
    # @param    application_info_list   A list of tuples.
    #                                   [(<application>,<host>,<service>), ...]
    # @return   A dictionary of application info and GUI skeleton pair.
    def getGUIOfApplication(self, application_info_list):
    
        return_value = {}
    
        query_string = """
            SELECT
                relationship__Analytic_Application__scheduled_by__Host__provides__Service.GUI_skeleton
            FROM
                entity__Analytic_Application,
                entity__Host,
                entity__Service,
                gerund__Host__provides__Service,
                relationship__Analytic_Application__scheduled_by__Host__provides__Service
            WHERE
                entity__Analytic_Application.Id = ?
                AND
                entity__Host.Id = ?
                AND
                entity__Service.Id = ?
                AND
                entity__Host.Id = gerund__Host__provides__Service.host_Id
                AND
                gerund__Host__provides__Service.service_Id = entity__Service.Id
                AND
                entity__Analytic_Application.Id = relationship__Analytic_Application__scheduled_by__Host__provides__Service.analytic_application_Id
                AND
                relationship__Analytic_Application__scheduled_by__Host__provides__Service.host_provides_service_host_Id = gerund__Host__provides__Service.host_Id
                AND
                relationship__Analytic_Application__scheduled_by__Host__provides__Service.host_provides_service_service_Id = gerund__Host__provides__Service.service_Id
                
            """
            
        for one_application in application_info_list:
        
            result = \
                self.db_operator.query_single(
                    query_string,
                    (one_application[0],
                    one_application[1],
                    one_application[2]))
            #print("GUI: " + str(result))
            
            return_value[str(one_application)] = result[0][0]
            
        # end of for
        
        return return_value
    
    # end of function getGUIOfApplication
    
    ##
    # @brief    Add an annotation and related info to a path.
    #
    def addAnnotation(self, annotation_info_list):
    
        return_value = {}
        
        for one_info in annotation_info_list:
        
            self.db_operator.transaction_begin()
            
            # find host, pvt user Id
            result = self.getHostUserIdOfResource([one_info[2]])
            host_Id = result[one_info[2]][0]
            private_user_Id = result[one_info[2]][1]
            #print("h: " + str(host_Id) + ", priv usr: " + str(private_user_Id))
            
            # TO DO
            # check if directory_annotation exists
            
            # prepare directory_annotation Id
            query_string = """
            
                SELECT 
                    MAX(entity_weak__Directory_Annotation.Id)
                FROM
                    entity_weak__Directory_Annotation
                """
            result = self.db_operator.query_single(query_string)
            
            directory_annotation_Id = result[0][0] + 1
            #print("dir antn Id: " + str(directory_annotation_Id))
            
            # create directory_annotation
            query_string = """
                
                INSERT INTO 
                    entity_weak__Directory_Annotation 
                    (
                        host_Id, 
                        private_user_Id,
                        private_resource_Id, 
                        Id,
                        relative_path,
                        sync_time
                    ) 
                    VALUES (?,?,?,?,?,?)
                """
            current_time = int(time.strftime("%Y%m%d", time.gmtime()))
            #print("curr time: " + str(current_time))
            self.db_operator.query_single(
                query_string,
                (host_Id, private_user_Id, one_info[2],
                directory_annotation_Id,
                one_info[3],
                current_time))
            
            # associate the directory_annotation to private resource
            query_string = """
                INSERT INTO 
                    relationship_weak__Private_Resource__has__Directory_Annotation 
                    (
                        private_resource_host_Id,
                        private_resource_private_user_Id, 
                        private_resource_Id,
                        directory_annotation_host_Id, 
                        directory_annotation_private_user_Id,
                        directory_annotation_private_resource_Id,
                        directory_annotation_Id
                    ) 
                    VALUES (?,?,?,?,?,?,?)
                """
            self.db_operator.query_single(
                query_string,
                (host_Id, private_user_Id, one_info[2],
                host_Id, private_user_Id, one_info[2], directory_annotation_Id))
            
            # prepare annotation Id
            query_string = """
            
                SELECT 
                    MAX(entity_weak__Annotation.Id)
                FROM
                    entity_weak__Annotation
                """
            result = self.db_operator.query_single(query_string)
        
            annotation_Id = result[0][0] + 1
            #print("antn Id: " + str(annotation_Id))
        
            query_string = """
            
                INSERT INTO entity_weak__Annotation 
                (
                    host_Id, 
                    private_user_Id,
                    private_resource_Id, 
                    directory_annotation_Id,
                    Id,
                    annotation
                ) 
                VALUES (?,?,?,?,?,?)
                    
                """
            self.db_operator.query_single(
                query_string,
                (host_Id,
                private_user_Id,
                one_info[2],
                directory_annotation_Id,
                annotation_Id,
                str(one_info[1])))
            
            return_value[str(one_info[0])] = annotation_Id
            
            # associate annotation with directory_annotation
            query_string = """
            
                INSERT INTO 
                    relationship_weak__Directory_Annotation__has__Annotation 
                    (
                        directory_annotation_host_Id,
                        directory_annotation_private_user_Id, 
                        directory_annotation_private_resource_Id,
                        directory_annotation_Id,
                        annotation_host_Id,
                        annotation_private_user_Id, 
                        annotation_private_resource_Id,
                        annotation_directory_annotation_Id,
                        annotation_Id
                    ) 
                VALUES (?,?,?,?,?,?,?,?,?)
                """
                
            self.db_operator.query_single(
                query_string,
                (host_Id, private_user_Id, one_info[2], directory_annotation_Id,
                host_Id, private_user_Id, one_info[2], directory_annotation_Id,
                annotation_Id))
                
            self.db_operator.transaction_commit()
                
        # end of for
        
        return return_value
    
    # end of function addAnnotation
    
    ##
    # @brief    List all annotations of a path which are visible to a user.
    #
    def listAnnotation(self, list_info_list):
    
        pass
    
    # end of function listAnnotation
    
    ##
    # @brief    Edit an annotation of a path.
    #
    def editAnnotation(self, edit_info_list):
    
        pass
    
    # end of function editAnnotation
    
    ##
    # @brief    Remove an annotation of a path.
    #
    def removeAnnotation(self, remove_info_list):
    
        pass
    
    # end of function removeAnnotation
    
    ##
    # @brief    Add an ACL rule to a path/annotation.
    #
    def addACL(self, add_info_list):
    
        return_value = {}
    
        for one_info in add_info_list:
        
            self.db_operator.transaction_begin()
        
            if one_info["ACL_type"] == "directory":
            
                # find host, pvt user Id
                result = self.getHostUserIdOfResource([int(one_info["resource"])])
                host_Id = result[int(one_info["resource"])]["host"]
                private_user_Id = result[int(one_info["resource"])]["user"]
                #print("h: " + str(host_Id) + \
                #    ", priv usr: " + str(private_user_Id))
 
                query_string = """
            
                    SELECT 
                        MAX(entity_weak__Directory_ACL.Id)
                    FROM
                        entity_weak__Directory_ACL
                    """
                result = self.db_operator.query_single(query_string)
            
                Id = result[0][0] + 1
                #print("dir_ACL Id: " + str(Id))
                
                #add Directory_ACL
                try:
                    # TO DO
                    #distinguish constraint unique failure 
                    #from primary key failure
                
                    query_string = """
                        INSERT INTO 
                            entity_weak__Directory_ACL 
                            (
                                host_Id, 
                                private_user_Id,
                                private_resource_Id, 
                                Id,
                                relative_path,
                                sync_time
                            ) 
                            VALUES (?,?,?,?,?,?)
                        """
                    current_time = int(time.strftime("%Y%m%d", time.gmtime()))
                    self.db_operator.query_single(
                        query_string,
                        (host_Id, private_user_Id, int(one_info["resource"]), 
                        Id, str(one_info["relative path"]),
                        current_time))
                        
                    query_string = """
                        INSERT INTO 
                            relationship_weak__Private_Resource__has__Directory_ACL 
                            (
                                private_resource_host_Id,
                                private_resource_private_user_Id, 
                                private_resource_Id,
                                directory_ACL_host_Id, 
                                directory_ACL_private_user_Id,
                                directory_ACL_private_resource_Id,
                                directory_ACL_Id
                            ) 
                            VALUES (?,?,?,?,?,?,?)
                        """
                    self.db_operator.query_single(
                        query_string,
                        (host_Id, private_user_Id, int(one_info["resource"]), 
                        host_Id, private_user_Id, int(one_info["resource"]), 
                        Id))
                    
                except Exception as e:
                    print("Exception, " + str(e) + ": " + str(e.message) + \
                        " while inserting into Directory with ACL!")
                    # retrieve existing Directory_ACL Id
                    query_string = """
                    
                        SELECT
                            entity_weak__Directory_ACL.Id
                        FROM
                            entity_weak__Directory_ACL
                        WHERE
                            entity_weak__Directory_ACL.private_resource_Id = ?
                            AND
                            entity_weak__Directory_ACL.relative_path = ?
                        """
                    result = \
                        self.db_operator.query_single(
                            query_string,
                            (int(one_info["resource"]), 
                            one_info["relative path"]))
                    Id = result[0][0]
                    
                if one_info["target_type"] == "group":
                
                    #add restricted_by_ACL
                    query_string = """
                        INSERT INTO 
                            relationship__BICLSU_Group__restricted_by_ACL__Directory_ACL
                            (
                                biclsu_group_Id,
                                IO_privilege,
                                computing_privilege,
                                recursive, 
                                directory_ACL_host_Id, 
                                directory_ACL_private_user_Id,
                                directory_ACL_private_resource_Id,
                                directory_ACL_Id
                            ) 
                            VALUES (?,?,?,?,?,?,?,?)
                        """
                    self.db_operator.query_single(
                        query_string,
                        (int(one_info["target_Id"]), 
                        int(one_info["IO_privilege"]), 
                        int(one_info["computing_privilege"]), 
                        int(one_info["recursive"]),
                        host_Id, private_user_Id, int(one_info["resource"]), 
                        Id))
                        
                    key = \
                        str((str(one_info["ACL_type"]),
                            str(one_info["path"]),
                            int(one_info["IO_privilege"]),
                            int(one_info["computing_privilege"]),
                            str(one_info["target_type"]),
                            int(one_info["target_Id"]),
                            int(one_info["recursive"])))
                    return_value[key] = Id
                
                elif one_info["target_type"] == "user":
                    pass
                # end if one_info["target_type"]
                
            elif one_info["ACL_type"] == "annotation":
            
                if one_info["target_type"] == "group":
                    pass
                elif one_info["target_type"] == "user":
                    pass
                # end if one_info["target_type"]
                
            # end if one_info["ACL_type"]
            
            self.db_operator.transaction_commit()
        
        # end of for
        
        return return_value
    
    # end of function addACL
    
    ##
    # @brief    List all ACL rules of a path/annotation 
    #           which are related to a user.
    #
    def listACL(self, list_info_list):
    
        return_value = {}
    
        for one_info in list_info_list:
            
            # find host, pvt user Id
            result = self.getHostUserIdOfResource([int(one_info["resource"])])
            host_Id = result[int(one_info["resource"])]["host"]
            private_user_Id = result[int(one_info["resource"])]["user"]
            #print("h: " + str(host_Id) + \
            #    ", priv usr: " + str(private_user_Id))
            
            if one_info["ACL_type"] == "directory":
            
                # find dir_ACL Id for path
                # TO DO
                #use complete query
                query_string = """
                
                    SELECT
                        entity_weak__Directory_ACL.Id
                    FROM
                        entity_weak__Directory_ACL
                    WHERE
                        entity_weak__Directory_ACL.host_Id = ?
                        AND
                        entity_weak__Directory_ACL.private_user_Id = ?
                        AND
                        entity_weak__Directory_ACL.private_resource_Id = ?
                        AND
                        entity_weak__Directory_ACL.relative_path = ?
                    """
                result = \
                self.db_operator.query_single(
                        query_string,
                        (host_Id, private_user_Id, int(one_info["resource"]),
                        str(one_info["relative path"])))
                if not result:
                    key = \
                        str((str(one_info["ACL_type"]),
                            str(one_info["path"])))
                    return_value[key] = []
                    continue
                Id = result[0][0]
                #print("dir_ACL Id: " + str(Id))
            
                # list group ACL union user ACL for dir_ACL
                query_string = """
                
                    SELECT
                        "user",
                        entity__BICLSU_User.Id,
                        entity__BICLSU_User.display_name,
                        entity_weak__Directory_ACL.Id,
                        relationship__BICLSU_User__restricted_by_ACL__Directory_ACL.IO_privilege,
                        relationship__BICLSU_User__restricted_by_ACL__Directory_ACL.computing_privilege,
                        relationship__BICLSU_User__restricted_by_ACL__Directory_ACL.recursive
                    FROM
                        entity__BICLSU_User,
                        entity_weak__Directory_ACL,
                        relationship__BICLSU_User__restricted_by_ACL__Directory_ACL
                    WHERE
                        relationship__BICLSU_User__restricted_by_ACL__Directory_ACL.directory_ACL_Id = ?
                        AND
                        entity__BICLSU_User.Id = relationship__BICLSU_User__restricted_by_ACL__Directory_ACL.biclsu_user_Id
                        AND
                        relationship__BICLSU_User__restricted_by_ACL__Directory_ACL.directory_ACL_host_Id = entity_weak__Directory_ACL.host_Id
                        AND
                        relationship__BICLSU_User__restricted_by_ACL__Directory_ACL.directory_ACL_private_user_Id = entity_weak__Directory_ACL.private_user_Id
                        AND
                        relationship__BICLSU_User__restricted_by_ACL__Directory_ACL.directory_ACL_private_resource_Id = entity_weak__Directory_ACL.private_resource_Id
                        AND
                        relationship__BICLSU_User__restricted_by_ACL__Directory_ACL.directory_ACL_Id = entity_weak__Directory_ACL.Id
                        
                    UNION
                    
                    SELECT
                        "group",
                        entity__BICLSU_Group.Id,
                        entity__BICLSU_Group.display_name,
                        entity_weak__Directory_ACL.Id,
                        relationship__BICLSU_Group__restricted_by_ACL__Directory_ACL.IO_privilege,
                        relationship__BICLSU_Group__restricted_by_ACL__Directory_ACL.computing_privilege,
                        relationship__BICLSU_Group__restricted_by_ACL__Directory_ACL.recursive
                    FROM
                        entity__BICLSU_Group,
                        entity_weak__Directory_ACL,
                        relationship__BICLSU_Group__restricted_by_ACL__Directory_ACL
                    WHERE
                        relationship__BICLSU_Group__restricted_by_ACL__Directory_ACL.directory_ACL_Id = ?
                        AND
                        entity__BICLSU_Group.Id = relationship__BICLSU_Group__restricted_by_ACL__Directory_ACL.biclsu_group_Id
                        AND
                        relationship__BICLSU_Group__restricted_by_ACL__Directory_ACL.directory_ACL_host_Id = entity_weak__Directory_ACL.host_Id
                        AND
                        relationship__BICLSU_Group__restricted_by_ACL__Directory_ACL.directory_ACL_private_user_Id = entity_weak__Directory_ACL.private_user_Id
                        AND
                        relationship__BICLSU_Group__restricted_by_ACL__Directory_ACL.directory_ACL_private_resource_Id = entity_weak__Directory_ACL.private_resource_Id
                        AND
                        relationship__BICLSU_Group__restricted_by_ACL__Directory_ACL.directory_ACL_Id = entity_weak__Directory_ACL.Id
                    """
                result = \
                    self.db_operator.query_single(
                            query_string,
                            (Id, Id))
                #print("ACL ls: " + str(result))
                        
                key = \
                    str((str(one_info["ACL_type"]),
                        str(one_info["path"])))
                return_value[key] = []
                for one_result in result: 
                
                    return_value[key].append({
                        "directory": one_result[3],
                        "target_type": one_result[0],
                        "target_Id": one_result[1],
                        "display_name": one_result[2],
                        "IO_privilege": one_result[4],
                        "computing_privilege": one_result[5],
                        "recursive": one_result[6]})
            
            elif one_info["ACL_type"] == "annotation":
            
                # list group ACL union user ACL for antn
                pass  
                    
            # end of if
                        
        # end of for
        
        #print("ACL ls: " + str(return_value))
        return return_value
    
    # end of function listACL
    
    ##
    # @brief    Edit an ACL rule of a path.
    #
    def editACL(self, edit_info_list):
    
        return_value = {}
    
        for one_info in edit_info_list:
            
            if one_info["ACL_type"] == "directory":
            
                if one_info["target_type"] == "group":
                    
                    # TO DO
                    #use complete query
                    query_string = """
                    
                        UPDATE
                            relationship__BICLSU_Group__restricted_by_ACL__Directory_ACL
                        SET
                            IO_privilege = ?,
                            computing_privilege = ?
                        WHERE
                            relationship__BICLSU_Group__restricted_by_ACL__Directory_ACL.biclsu_group_Id = ?
                            AND
                            relationship__BICLSU_Group__restricted_by_ACL__Directory_ACL.directory_ACL_Id = ?
                        """
                    self.db_operator.query_single(
                        query_string,
                        (int(one_info["IO_privilege"]),
                        int(one_info["computing_privilege"]),
                        int(one_info["target_Id"]),
                        int(one_info["directory"])))
                    
                elif one_info["target_type"] == "user":
                    pass
                # end of if
            
            elif one_info["ACL_type"] == "annotation":
            
                if one_info["target_type"] == "group":
                    pass
                elif one_info["target_type"] == "user":
                    pass
                # end of if    
                    
            # end of if
                        
        # end of for
        
        return return_value
    
    # end of function editACL
    
    ##
    # @brief    Remove an ACL rule of a path.
    #
    def removeACL(self, remove_info_list):
    
        return_value = {}
    
        for one_info in remove_info_list:
            
            if one_info["ACL_type"] == "directory":
            
                if one_info["target_type"] == "group":
                    
                    # TO DO
                    #use complete query
                    query_string = """
                    
                        DELETE FROM
                            relationship__BICLSU_Group__restricted_by_ACL__Directory_ACL
                        WHERE
                            relationship__BICLSU_Group__restricted_by_ACL__Directory_ACL.biclsu_group_Id = ?
                            AND
                            relationship__BICLSU_Group__restricted_by_ACL__Directory_ACL.directory_ACL_Id = ?
                        """
                    self.db_operator.query_single(
                        query_string,
                        (int(one_info["target_Id"]),
                        int(one_info["directory"])))
                        
                    query_string = """
                    
                        DELETE FROM
                            entity_weak__Directory_ACL
                        WHERE
                            NOT EXISTS (
                                SELECT
                                    1
                                FROM
                                    relationship__BICLSU_Group__restricted_by_ACL__Directory_ACL
                                WHERE
                                    relationship__BICLSU_Group__restricted_by_ACL__Directory_ACL.directory_ACL_Id = entity_weak__Directory_ACL.Id)
                        """
                    self.db_operator.query_single(query_string)
                    
                elif one_info["target_type"] == "user":
                    pass
                # end of if
            
            elif one_info["ACL_type"] == "annotation":
            
                if one_info["target_type"] == "group":
                    pass
                elif one_info["target_type"] == "user":
                    pass
                # end of if    
                    
            # end of if
                        
        # end of for
        
        return return_value
    
    # end of function removeACL
    
    ##
    # @brief    List all paths which allow specified accesses.
    #
    # @param    request_list    A list of dictionary.
    #                           [{"user": <SSO Id>, 
    #                           "IO_access": 1/2, "computing_access": 1}, ...]
    #
    # @return   A dictionary of lists.
    #           {"(<SSO Id>,<IO acc>,<comp acc>)": 
    #           ["resource": <resc path>, "relative": <relative path>}, ...],
    #           , ...}
    def getAllAccessiblePathFromOthers(self, request_list):
    
        return_value = {}
    
        # TO DO
        # support autonamous region
    
        query_IO = """
            SELECT
                owner.Id,
                entity_weak__Private_Resource.Id,
                entity_weak__Directory_ACL.relative_path
            FROM
                entity__BICLSU_User
                AS requester,
                entity__BICLSU_User
                AS owner,
                entity_weak__Private_User,
                entity_weak__Private_Resource,
                entity_weak__Directory_ACL,
                relationship_weak__Private_User__has__Private_Resource,
                relationship__BICLSU_User__maps_to__Private_User,
                relationship_weak__Private_Resource__has__Directory_ACL,
                relationship__BICLSU_User__restricted_by_ACL__Directory_ACL
            WHERE
                requester.myLSU_Id = ?
                AND
                relationship__BICLSU_User__restricted_by_ACL__Directory_ACL.IO_privilege >= ?
                AND
                requester.Id = relationship__BICLSU_User__restricted_by_ACL__Directory_ACL.biclsu_user_Id
                AND
                relationship__BICLSU_User__restricted_by_ACL__Directory_ACL.directory_ACL_host_Id = entity_weak__Directory_ACL.host_Id
                AND
                relationship__BICLSU_User__restricted_by_ACL__Directory_ACL.directory_ACL_private_user_Id = entity_weak__Directory_ACL.private_user_Id
                AND
                relationship__BICLSU_User__restricted_by_ACL__Directory_ACL.directory_ACL_private_resource_Id = entity_weak__Directory_ACL.private_resource_Id
                AND
                relationship__BICLSU_User__restricted_by_ACL__Directory_ACL.directory_ACL_Id = entity_weak__Directory_ACL.Id
                AND
                entity_weak__Private_Resource.host_Id = relationship_weak__Private_Resource__has__Directory_ACL.private_resource_host_Id
                AND
                entity_weak__Private_Resource.private_user_Id = relationship_weak__Private_Resource__has__Directory_ACL.private_resource_private_user_Id
                AND
                entity_weak__Private_Resource.Id = relationship_weak__Private_Resource__has__Directory_ACL.private_resource_Id
                AND
                relationship_weak__Private_Resource__has__Directory_ACL.directory_ACL_host_Id = entity_weak__Directory_ACL.host_Id
                AND
                relationship_weak__Private_Resource__has__Directory_ACL.directory_ACL_private_user_Id = entity_weak__Directory_ACL.private_user_Id
                AND
                relationship_weak__Private_Resource__has__Directory_ACL.directory_ACL_private_resource_Id = entity_weak__Directory_ACL.private_resource_Id
                AND
                relationship_weak__Private_Resource__has__Directory_ACL.directory_ACL_Id = entity_weak__Directory_ACL.Id
                AND
                entity_weak__Private_User.host_Id = relationship_weak__Private_User__has__Private_Resource.private_user_host_Id
                AND
                entity_weak__Private_User.Id = relationship_weak__Private_User__has__Private_Resource.private_user_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_host_Id = entity_weak__Private_Resource.host_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_private_user_Id = entity_weak__Private_Resource.private_user_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_Id = entity_weak__Private_Resource.Id
                AND
                owner.Id = relationship__BICLSU_User__maps_to__Private_User.biclsu_user_Id
                AND
                relationship__BICLSU_User__maps_to__Private_User.private_user_host_Id = entity_weak__Private_User.host_Id
                AND
                relationship__BICLSU_User__maps_to__Private_User.private_user_Id = entity_weak__Private_User.Id
            
            UNION
            
            SELECT
                owner.Id,
                entity_weak__Private_Resource.Id,
                entity_weak__Directory_ACL.relative_path
            FROM
                entity__BICLSU_User
                AS requester,
                entity__BICLSU_User
                AS owner,
                entity__BICLSU_Group,
                entity_weak__Private_User,
                entity_weak__Private_Resource,
                entity_weak__Directory_ACL,
                relationship__BICLSU_Group__consists_of__BICLSU_User,
                relationship_weak__Private_User__has__Private_Resource,
                relationship__BICLSU_User__maps_to__Private_User,
                relationship_weak__Private_Resource__has__Directory_ACL,
                relationship__BICLSU_Group__restricted_by_ACL__Directory_ACL
            WHERE
                requester.myLSU_Id = ?
                AND
                requester.Id = relationship__BICLSU_Group__consists_of__BICLSU_User.biclsu_user_Id
                AND
                relationship__BICLSU_Group__consists_of__BICLSU_User.biclsu_group_Id = entity__BICLSU_Group.Id
                AND
                relationship__BICLSU_Group__restricted_by_ACL__Directory_ACL.IO_privilege >= ?
                AND
                entity__BICLSU_Group.Id = relationship__BICLSU_Group__restricted_by_ACL__Directory_ACL.biclsu_group_Id
                AND
                relationship__BICLSU_Group__restricted_by_ACL__Directory_ACL.directory_ACL_host_Id = entity_weak__Directory_ACL.host_Id
                AND
                relationship__BICLSU_Group__restricted_by_ACL__Directory_ACL.directory_ACL_private_user_Id = entity_weak__Directory_ACL.private_user_Id
                AND
                relationship__BICLSU_Group__restricted_by_ACL__Directory_ACL.directory_ACL_private_resource_Id = entity_weak__Directory_ACL.private_resource_Id
                AND
                relationship__BICLSU_Group__restricted_by_ACL__Directory_ACL.directory_ACL_Id = entity_weak__Directory_ACL.Id
                AND
                entity_weak__Private_Resource.host_Id = relationship_weak__Private_Resource__has__Directory_ACL.private_resource_host_Id
                AND
                entity_weak__Private_Resource.private_user_Id = relationship_weak__Private_Resource__has__Directory_ACL.private_resource_private_user_Id
                AND
                entity_weak__Private_Resource.Id = relationship_weak__Private_Resource__has__Directory_ACL.private_resource_Id
                AND
                relationship_weak__Private_Resource__has__Directory_ACL.directory_ACL_host_Id = entity_weak__Directory_ACL.host_Id
                AND
                relationship_weak__Private_Resource__has__Directory_ACL.directory_ACL_private_user_Id = entity_weak__Directory_ACL.private_user_Id
                AND
                relationship_weak__Private_Resource__has__Directory_ACL.directory_ACL_private_resource_Id = entity_weak__Directory_ACL.private_resource_Id
                AND
                relationship_weak__Private_Resource__has__Directory_ACL.directory_ACL_Id = entity_weak__Directory_ACL.Id
                AND
                entity_weak__Private_User.host_Id = relationship_weak__Private_User__has__Private_Resource.private_user_host_Id
                AND
                entity_weak__Private_User.Id = relationship_weak__Private_User__has__Private_Resource.private_user_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_host_Id = entity_weak__Private_Resource.host_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_private_user_Id = entity_weak__Private_Resource.private_user_Id
                AND
                relationship_weak__Private_User__has__Private_Resource.private_resource_Id = entity_weak__Private_Resource.Id
                AND
                owner.Id = relationship__BICLSU_User__maps_to__Private_User.biclsu_user_Id
                AND
                relationship__BICLSU_User__maps_to__Private_User.private_user_host_Id = entity_weak__Private_User.host_Id
                AND
                relationship__BICLSU_User__maps_to__Private_User.private_user_Id = entity_weak__Private_User.Id
            """
            
        query_computing = """
            SELECT
            FROM
                relationship__BICLSU_User__restricted_by_ACL__Directory_ACL
            WHERE
                relationship__BICLSU_User__restricted_by_ACL__Directory_ACL.computing_privilege = ?
                
            UNION
            
            
            """
    
        for one_request in request_list:
            if "IO_access" in one_request:
                result_list = \
                    self.db_operator.query_single(
                        query_IO, 
                        (str(one_request["user"]),
                        int(one_request["IO_access"]),
                        str(one_request["user"]),
                        int(one_request["IO_access"])))
                        
            elif "computing_access" in one_request:
                result_list = \
                    self.db_operator.query_single(
                        query_computing, 
                        (str(one_request["user"]),
                        int(one_request["computing_access"]),
                        str(one_request["user"]),
                        int(one_request["computing_access"])))
            #print("result: " + str(result_list))
                        
                        
            key = \
                str((str(one_request["user"]),
                    int(one_request["IO_access"]),
                    int(one_request["computing_access"])))
            
            return_value[key] = []
            for one_result in result_list:
                return_value[key].append({
                    "resource": 
                        "/0/"+ str(one_result[0]) +"/"+ str(one_result[1]) +"/",
                    "relative": str(one_result[2])})
        
        # end of for
        
        return return_value
    
    # end of function getAllAccessiblePathFromOthers
    
    def checkPermissionForPath(self):
    
        pass
    
    # end of function checkPermissionForPath

# end of class BICLSU_DB_Operator
