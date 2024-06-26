from random import choice
import consul
import json
import uuid

consul_client = consul.Consul(host="192.168.69.104", port=8500)
def Register_service(name, port):
    id=str(uuid.uuid4())
    consul_client.agent.service.register(name,id,address="192.168.69.104",port=port)
    return id
def Deregister_service(id):
    consul_client.agent.service.deregister(id)
def Get_port(service_name):
    ports=[]
    for _, service_info in consul_client.agent.services().items():
        if service_info['Service'] == service_name:
            ports.append(service_info['Port'])
    port_result = choice(ports)
    result=f"http://127.0.0.1:{0}/data".format(port_result)
    return result
def Add_value(key, value):
    value_json = json.dumps(value)
    consul_client.kv.put(key, value_json)
def Get_value(key):
    _, config = consul_client.kv.get(key)
    return config['Value']
