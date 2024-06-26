from flask import Flask, request, jsonify
import random
import requests
import hazelcast
from consul_lib import *

# Реєстрація сервісу в Consul
service_id = Register_service('facade-service', 5000)
print("Registered")

# Отримання конфігурації Hazelcast з Consul
hz_config = json.loads(Get_value('HZ_config'))
print("Hazelcast config: ", hz_config)

# Ініціалізація клієнта Hazelcast та отримання черги повідомлень
hz = hazelcast.HazelcastClient(cluster_name=hz_config['cluster_name'], cluster_members=[])
messages_queue = hz.get_queue(hz_config['queue_name']).blocking()

# Генерація унікального ключа для повідомлення
def generate_unique_key():
   return ''.join(random.choices('123456789', k=4))

app = Flask(__name__)

@app.route('/data', methods=['GET', 'POST'])
def handle_data():
    # Отримання IP-адрес із Consul для сервісів логування та повідомлень
    logging_ip = Get_port("logging-service")
    message_ip = Get_port("message-service")
    
    if request.method == 'GET':
        # Відправлення GET-запитів на сервіси логування та повідомлень
        response_logging = requests.get(logging_ip, timeout=5)
        response_logging.raise_for_status()
        response_message = requests.get(message_ip).text
        return jsonify({'Message data': response_message, 'Log data': response_logging.text})
    
    elif request.method == 'POST':
        # Обробка POST-запиту на збереження повідомлення
        message = request.get_data().decode('utf-8')
        
        # Додавання повідомлення до черги в Hazelcast
        messages_queue.offer(message)
        
        # Генерація унікального ключа та створення даних для логування
        key = generate_unique_key()
        data = {"key": key, "msg": message}
        print(data)
        
        # Відправлення даних на сервіс логування
        response = requests.post(logging_ip, data=data)
        print("Response:", response.text)
        data = response.text
        return data

if __name__ == '__main__':
    # Запуск Flask додатка на вказаному порту
    app.run(debug=True, port=5000)
    
    # Вивід ідентифікатора поточного сервісу у Consul
    print("Consul Current_ID:", service_id)
    input()
    
    # Видалення реєстрації сервісу після закінчення роботи
    Deregister_service(service_id)
