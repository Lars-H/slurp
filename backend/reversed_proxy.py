class Reversed_Proxy(object):

    def __init__(self, app, script_name=None, scheme='http', server=None):
        self.app = app
        self.script_name = script_name
        self.scheme = scheme
        self.server = server

    def __call__(self, environ, start_response):
        script_name = environ.get('HTTP_X_SCRIPT_NAME', '') or self.script_name
        if script_name:
            environ['SCRIPT_NAME'] = script_name
            path_info = environ['PATH_INFO']
            if path_info.startswith(script_name):
                environ['PATH_INFO'] = path_info[len(script_name):]
        #scheme = environ.get('HTTP_X_SCHEME', '') or self.scheme
        scheme = environ.get('HTTP_X_FORWARDED_PROTO', '') or self.scheme
        if scheme:
            environ['wsgi.url_scheme'] = scheme
        server = environ.get('HTTP_X_FORWARDED_SERVER', '') or self.server
        if server:
            environ['HTTP_HOST'] = server
        environ['wsgi.url_scheme'] = self.scheme
        return self.app(environ, start_response)