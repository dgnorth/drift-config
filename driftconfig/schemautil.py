# -*- coding: utf-8 -*-
'''

Schema Util - Helpers to pretty print Json schema errors and other bits.

'''
import logging

import jsonschema
from json import dumps
from six.moves import cStringIO as StringIO
from click import echo

log = logging.getLogger(__name__)


def check_schema(json_object, schema, title=None):
    """Do json schema check on object and abort with 400 error if it fails."""
    try:
        jsonschema.validate(json_object, schema, format_checker=jsonschema.FormatChecker())
        ###jsonschema.validate(json_object, schema)
    except jsonschema.ValidationError as e:
        report = _generate_validation_error_report(e, json_object)
        if title:
            report = "Schema check failed: %s\n%s" % (title, report)
        e.message = report
        raise

def _generate_validation_error_report(e, json_object):
    """Generate a detailed report of a schema validation error."""

    # Discovering the location of the validation error is not so straight
    # forward:
    # 1. Traverse the json object using the 'path' in the validation exception
    #    and replace the offending value with a special marker.
    # 2. Pretty-print the json object indendented json text.
    # 3. Search for the special marker in the json text to 3 the actual
    #    line number of the error.
    # 4. Make a report by showing the error line with a context of
    #   'lines_before' and 'lines_after' number of lines on each side.

    if json_object is None:
        return "Request requires a JSON body"
    if not e.path:
        return str(e)
    marker = "3fb539de-ef7c-4e29-91f2-65c0a982f5ea"
    lines_before = 7
    lines_after = 7

    # Find the error object and replace it with the marker
    o = json_object
    for entry in list(e.path)[:-1]:
        o = o[entry]
    try:
        orig, o[e.path[0]] = o[e.path[0]], marker
    except:
        # TODO: report the error
        echo("Error setting marker in schemachecker!")

    # Pretty print the object and search for the marker
    json_error = dumps(json_object, indent=4)
    io = StringIO(json_error)

    errline = None
    for lineno, text in enumerate(io):
        if marker in text:
            errline = lineno
            break

    if errline is not None:
        # re-create report
        report = []
        json_object[e.path[0]] = orig
        json_error = dumps(json_object, indent=4)
        io = StringIO(json_error)

        for lineno, text in enumerate(io):
            if lineno == errline:
                line_text = "{:4}: >>>".format(lineno + 1)
            else:
                line_text = "{:4}:    ".format(lineno + 1)
            report.append(line_text + text.rstrip("\n"))

        report = report[
            max(0, errline - lines_before):errline + 1 + lines_after]

        s = "Error in line {}:\n".format(errline + 1)
        s += "\n".join(report)
    else:
        s = str(e)

    return s
