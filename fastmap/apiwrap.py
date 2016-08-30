#!/usr/bin/env python

import time
import os
import sys
import uuid  # @UnusedImport
import platform
import logging

from pgoapi import PGoApi
from pgoapi.exceptions import AuthException

log = logging.getLogger(__name__)


class AccountBannedException(AuthException):
    pass

class PoGoAccount():
    def __init__(self, auth, login, passw):
        self.auth_service = auth
        self.username = login
        self.password = passw

def api_init(account):
    api = PGoApi()
    
    try:
        api.set_position(360,360,0)  
        api.set_authentication(provider = account.auth_service,\
                               username = account.username, password =  account.password)
        api.activate_signature(get_encryption_lib_path()); time.sleep(1); api.get_player()
    
    except AuthException:
        log.error('Login for %d:%d failed - wrong credentials?' % (account.username, account.password))
        return None
    
    else:
        time.sleep(1); response = api.get_inventory()
        
        if response:
            if 'status_code' in response:
                if response['status_code'] == 1 or response['status_code'] == 2: return api
                
                elif response['status_code'] == 3:
                    # try to accept ToS
                    time.sleep(5); response = api.mark_tutorial_complete(tutorials_completed = 0,\
                                    send_marketing_emails = False, send_push_notifications = False)                    

                    if response['status_code'] == 1 or response['status_code'] == 2:
                        print('Accepted TOS for %s' % account.username)
                        return api
                    
                    elif response['status_code'] == 3:
                        print('Account %s BANNED!' % account.username)
                        raise AccountBannedException; return None
                
    return None
        
def check_reponse(response):
  
    if response:
        if 'responses' in response and 'status_code' in response:
            if response['status_code'] == 1 or response['status_code'] == 2:
                return response
            elif response['status_code'] == 3:
                raise AccountBannedException; return None
        else: return None
    
    return None

def get_encryption_lib_path():
    # win32 doesn't mean necessarily 32 bits
    if sys.platform == "win32" or sys.platform == "cygwin":
        if platform.architecture()[0] == '64bit':
            lib_name = "encrypt64bit.dll"
        else:
            lib_name = "encrypt32bit.dll"

    elif sys.platform == "darwin":
        lib_name = "libencrypt-osx-64.so"

    elif os.uname()[4].startswith("arm") and platform.architecture()[0] == '32bit':  # @UndefinedVariable
        lib_name = "libencrypt-linux-arm-32.so"

    elif os.uname()[4].startswith("aarch64") and platform.architecture()[0] == '64bit':  # @UndefinedVariable
        lib_name = "libencrypt-linux-arm-64.so"

    elif sys.platform.startswith('linux'):
        if "centos" in platform.platform():
            if platform.architecture()[0] == '64bit':
                lib_name = "libencrypt-centos-x86-64.so"
            else:
                lib_name = "libencrypt-linux-x86-32.so"
        else:
            if platform.architecture()[0] == '64bit':
                lib_name = "libencrypt-linux-x86-64.so"
            else:
                lib_name = "libencrypt-linux-x86-32.so"

    elif sys.platform.startswith('freebsd'):
        lib_name = "libencrypt-freebsd-64.so"

    else:
        err = "Unexpected/unsupported platform '{}'".format(sys.platform)
        log.error(err)
        raise Exception(err)
    
    # check for lib in root dir or PATH
    if os.path.isfile(lib_name):
        return lib_name
    
    test_paths = ["../pgoapi/magiclib","../pgoapi/libencrypt","../magiclib","../libencrypt"]
    
    for test_path in test_paths:
        lib_path = os.path.join(os.path.dirname(__file__), test_path, lib_name)
        if os.path.isfile(lib_path): return lib_path

    err = "Could not find [{}] encryption library '{}'".format(sys.platform, lib_name)
    log.error(err)
    raise Exception(err)

    return None

def limit_cells(cells, limit=100):
    return cells[:limit]