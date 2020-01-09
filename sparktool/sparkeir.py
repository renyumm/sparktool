'''
@Date: 2019-12-07 18:52:48
@LastEditors  : ryan.ren
@LastEditTime : 2020-01-05 23:23:36
@Description: generate settings file
'''
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

            "version": '2.0.0'
        }
    
    # generate settings file
    if not os.path.exists(ini):
        with open(ini, 'w') as f:
            f.write(json.dumps(tmp, indent=4))
    
    # modifiy environment and syspath
    with open(ini, 'r') as f:
        settings = dict(json.load(f))
    
    if settings.get('version') != tmp['version']:
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


if 1==1:
    generate_settings()







