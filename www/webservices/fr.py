#!/usr/bin/python

import os
import sys
import re
import struct
import pymongo
from flight_recorder.mongo import FlightRecorderFetcher
from bottle import route, run, template, request

method_list = {}

def main():
    #create a new flight recorder instance
    fr = FlightRecorderFetcher()

    # provides a consistent way to return data via webservices
    def format_results( res, err=None ):
        error_bool = True if(err) else False
        return { 
            'results':   res,
            'error':     error_bool,
            'error_msg': err
        }
        
    # takes in a validator grabs the method_name and method_description keys
    # to build a help message to print to the user when /help is executed
    def create_method_help_obj( validators ):
        global method_list
       
        # grab our name and description and remove them from the validator dict
        method_name        = validators.pop('method_name', None)
        method_description = validators.pop('method_description', None)

        # make sure they are defined
        if(method_name is None):
            print "You must provide a method_name along with the validator for the /help method!"
            sys.exit(1)
        if(method_description is None):
            print "You must provide a method_description along with the validator for the /help method!"
            sys.exit(1)

        method_list[method_name] = {}
        method_list[method_name]['description'] = method_description
        method_list[method_name]['parameters']  = []

        for param in validators:
            parameter = {}
            validator = validators[param]
            parameter['name']     = param
            parameter['required'] = validator.get('required', False)

            if(validator.get('pattern', False)):
                parameter['pattern'] = validator.get('pattern')

            if(validator.get('type', False)):
                parameter['type'] = validator.get('type')

            if(validator.get('checks', False)):
                checks = validator.get('checks');
                parameter['checks'] = []
                for check in checks:
                    try:
                        parameter['checks'].append(check.__descr__)
                    except:
                        print "Must provide __descr__ for checks!"
                        sys.exit(1)
            method_list[method_name]['parameters'].append(parameter)

        method_list[method_name]['parameters'] = sorted( method_list[method_name]['parameters'], key=lambda k: k['name']) 

        return validators


    # special decorator that takes in kwargs where the key is the parameter
    # and the value is an object representing the validation it should do to the 
    # parameter
    def validate_params( **kwargs ):
        validators = create_method_help_obj(kwargs)
        def validate_params_decorator(func):
            def wrapper(*args, **kwargs):
                validated_args = {}
                for param in validators:
                    validator = validators[param]
                    checks    = validator.get('checks', [])
                    default   = validator.get('default', None)
                    value     = request.params.get(param, default)

                    # check if the param was required
                    if(validator.get('required', False) and value == None ):
                        return format_results( None, "Parameter, {0}, is required".format(param) )

                    # only do further validation if it's not required
                    if(value != None):
                        # if the parameter needs to match a particular pattern make sure it does
                        if( validator.get('pattern', False) ):
                            pattern = validator.get('pattern')
                            regex   = re.compile(pattern)
                            if(not regex.match(value) ): 
                                return format_results( None, "Parameter, {0}, must match pattern, {1}".format(param, pattern) )

                        # if a type is set try to convert to the type otherwise send error
                        if( validator.get('type', False) ):
                            if( validator.get('type') == 'string' ):
                                try: 
                                    value = str(value)
                                except Exception as e:
                                    return format_results( None, "Error converting {0} to string: {1}".format(value, e))
                            if( validator.get('type') == 'integer' ):
                                try: 
                                    value = int(value)
                                except Exception as e:
                                    return format_results( None, "Error converting {0} to integer: {1}".format(value, e))


                        # if the param has any special check perform them now
                        for check in checks:
                            err, msg = check( value )
                            if(err):
                                return format_results( None, msg )

                    # if we've gotten here the param is good so add it to our validated params
                    validated_args[param] = value


                return func(validated_args)
            return wrapper
        return validate_params_decorator

    @route('/')
    @route('/help')
    @validate_params(
        method_name = 'help',
        method_description = 'Returns information about what methods are available and their parameters if a method is specified',
        method = {}
    )

    def help(params):
        method = params.get('method')

        methods = None
        if(method is not None):
            try:
                methods = [method_list.get(method)]
                methods[0]['name'] = method
            except:
                return format_results( None, "Method, {0}, does not exists".format(method) )
        else:
            methods = method_list.keys()

        methods.sort()
        return format_results( methods )

    @route('/streams')
    @validate_params(
        method_name = 'streams',
        method_description = 'returns a list of streams matching specified params',
        name  = {},
        addr  = {'required': False, 'type': 'string'},
        port  = {'required': False, 'type': 'integer'},
        start = {'required': False, 'type': 'integer'},
        stop  = {'required': False, 'type': 'integer'},
    )
    def get_streams(params):
        results = fr.get_streams( params )
        if(results is not None):
            return format_results( results )
        else:
            return format_results( [], fr.get_error() )

    @route('/messages')
    @validate_params(
        method_name='messages',
        method_description = 'returns a list of messages matching the specified params',
        name = {},
        stream_id = {'required': False, 'type': 'string'},
        start = {'required': False, 'type': 'integer'},
        stop = {'required': False, 'type': 'integer'},
        )
    def get_messages(params):
        results = fr.get_messages( params )
        if(results is not None):
            return format_results( results )
        else:
            return format_results([], fr.get_error())


    run(host="0.0.0.0", port=8080)

main()
