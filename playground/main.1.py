url = 'zk://localhost:2181'
d = {
    "type": "SUBSCRIBE",
    "subscribe": {
        "framework_info": {
            "user": "username",
            "name": "Example HTTP Framework"
        }
    }
}


from malefico.core.scheduler import  MesosScheduler


sched = MesosScheduler(url,d)