from ingester3 import log

@log.log_ingester()
def log_tester(x=1, y=2):
    print(x+y)

for i in range(1000000):
    log_tester(1,i)
