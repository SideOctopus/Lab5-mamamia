import requests




while(True):
    print("\n*** Client Menu ***")
    print("1. Send POST request with a message")
    print("2. Send GET request to retrieve messages")
    print("3. Exit")
    choice = input("Enter your choice (1-3): ")
    
    if choice=="3":
        exit()
    elif choice=="1":
        data = input("Message:")
        response_post = requests.post('http://127.0.0.1:5000/data', data=data)
        print('Response:', response_post.text)
    elif choice=="2":
        response_get = requests.get('http://127.0.0.1:5000/data')
        print('Response:', response_get.json())
    else:
        print("Retry")
