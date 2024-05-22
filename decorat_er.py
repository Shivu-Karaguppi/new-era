import datetime 

def printfun(func):
    def wrapper():
        print("hola,",datetime.datetime.now())
        func()
        print(datetime.datetime.now())
    return wrapper

@printfun
def bat_man():
    print("joker")

bat_man()