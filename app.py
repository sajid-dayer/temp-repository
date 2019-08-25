import os

import cherrypy
import redis

from config import redis_host, redis_port, redis_db, redis_password, app_running_port, app_running_ip

redis_data = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db,
                               password=redis_password, health_check_interval=30)


class BseDataApp(object):
    @cherrypy.expose
    def index(self):
        return "Hello Zerodha!"

    def append_records_response_data(self, response, match, record_count):
        for i, name in enumerate(redis_data.scan_iter(match=match)):
            try:
                data = redis_data.hgetall(name)
                response["data"].append(
                    [data["SC_NAME"], data["OPEN"], data["CLOSE"], data["HIGH"], data["LOW"], data["LAST"],
                     data["PREVCLOSE"]])
                if i == record_count:
                    break
            except Exception as e:
                print e
        return response

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def search_records(self, name="DEFAULT"):
        remote_ip = cherrypy.request.remote.ip
        name = name.upper()
        response = {"data": []}
        if str(name) == "DEFAULT":
            response = self.append_records_response_data(response, "*", record_count=20)
        else:
            response = self.append_records_response_data(response, '*{0}*'.format(name), record_count=10)
        return response

    @cherrypy.expose
    def home(self):
        return open("static/html/table_page.html").read()


print os.getcwd()
print os.path.abspath(os.getcwd())
conf = {
    '/': {
        'tools.sessions.on': True,
        'tools.staticdir.root': os.path.abspath(os.getcwd())
    },
    '/static': {
        'tools.staticdir.on': True,
        'tools.staticdir.dir': './static'
    }
}
# cherrypy.config.update({'server.socket_port': app_running_port, 'server.socket_host': app_running_ip})
# cherrypy.quickstart(BseDataApp(), '/', conf)

cherrypy.config.update({'server.socket_port': app_running_port, 'server.socket_host': app_running_ip,'engine.autoreload.on': False})
cherrypy.server.unsubscribe()
cherrypy.engine.start()
wsgiapp = cherrypy.tree.mount(BseDataApp(),'/', conf)
# uwsgi --socket 0.0.0.0:64008 --protocol=http --wsgi-file app.py --callable wsgiapp
# http://13.232.47.5:64008/home_page
