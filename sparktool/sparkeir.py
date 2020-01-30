import sys
import os
import json


def generate_settings():
    ini = os.path.expanduser('~') + '/.sparktool.json'
    tmp = {
            "env": {
                "SPARK_HOME": "/opt/cloudera/parcels/SPARK2/lib/spark2",
                "JAVA_HOME": ""
            },

            "sys": [
                "/opt/cloudera/parcels/SPARK2/lib/spark2/python/lib/py4j-0.10.7-src.zip",
                "/opt/cloudera/parcels/SPARK2/lib/spark2/python/lib/pyspark.zip"
            ],

            "kudu": {
                "database": [],
                "kudumaster": "prod-hadoop-cdh5-02.homecredit.cn:7051"
            },

            "keytab": ["admin@EXAMPLE.COM", "keytabpath"],
            "hue": {"username": "", "password": ""},
            "hue_editor": 'http://prod-hadoop-cdh5-01.homecredit.cn:8888',
            "version": '3.0.7'
        }
    
    # generate settings file
    if not os.path.exists(ini):
        with open(ini, 'w') as f:
            f.write(json.dumps(tmp, indent=4))
    
    # modifiy environment and syspath
    with open(ini, 'r') as f:
        settings = dict(json.load(f))
    
    # sync version 
    if settings.get('version') != tmp['version']:
        settings['version'] = tmp['version']
        for key in tmp:
            if key not in settings:
                settings[key] = tmp[key]
        with open(ini, 'w') as f:
            f.write(json.dumps(settings, indent=4))

    for key in settings['env']:
        if settings['env'][key]:
            os.environ[key] = settings['env'][key]

    for key in settings['sys']:
        sys.path.insert(0, key)

    if settings['keytab'][0] != "admin@EXAMPLE.COM":
        os.system(
            'kinit -kt {0} {1}'.format(settings['keytab'][1], settings['keytab'][0]))
    else:
        os.system('kinit')

def switch_keytab(username, keytabpath, ifcover=True):
    '''
    @description: switch keytab
    '''
    os.system(
        'kinit -kt ' + keytabpath + ' ' + username
    )

    if ifcover:
        ini = os.path.expanduser('~') + '/.sparktool.json'
        with open(ini, 'r') as f:
            settings = dict(json.load(f))
            settings['keytab'][1] = keytabpath
            settings['keytab'][0] = username
        with open(ini, 'w') as f:
            f.write(json.dumps(settings, indent=4))

    print('Switch Successfully')


def switch_huetab(username, password):
    '''
    @description: switch keytab
    '''
    ini = os.path.expanduser('~') + '/.sparktool.json'
    with open(ini, 'r') as f:
        settings = dict(json.load(f))
        settings['hue']["username"] = username
        settings['hue']["password"] = password
    with open(ini, 'w') as f:
        f.write(json.dumps(settings, indent=4))

    print('Switch Successfully')


if 1==1:
    generate_settings()







