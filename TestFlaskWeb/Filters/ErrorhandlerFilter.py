import sys
import traceback
from Ext.Convert import *
from flask import jsonify

class ErrorhandlerFilter():
    def init_app(self,app):
        @app.errorhandler(Exception)
        def framework_error(e):
            result = ObjectErrorResult()
            return result

        def ObjectErrorResult():
            ex_type, ex_val, ex_stack = sys.exc_info()
            errors = []
            for stack in traceback.extract_tb(ex_stack):
                errors.append(str(stack))

            result = {}
            result["ex_type"] = str(ex_type)
            result["ex_val"] = str(ex_val)
            result["errors"] = errors
            return result

        def StrErrorResult():
            errors = []
            for stack in traceback.extract_tb(ex_stack):
                errors.append(str(stack))
        
            errors.append(str(ex_type))
            errors.append(str(ex_val))

            result = "".join(errors)
            return result