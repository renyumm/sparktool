from __future__ import print_function
import re
import prettytable as pt
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark import SparkConf
import sqlparse
import datetime
import os
import json
import sys
import re


class SparkCreator(object):
    def __init__(self, appname=None, pyv=None, **param):
        '''
        @description: init
        '''
        ini = os.path.expanduser('~') + '/.sparktool.json'
        with open(ini, 'r') as f:
            cfg = dict(json.load(f))

        self.__kudumaster = cfg['kudu'].get('kudumaster', '')
        self.__customviews = {'view_sch': set(), 'view_tbs': {}}
        self.__customkudus = {}
        self.__pyv = sys.version[0]
        self.spark = None

        if pyv:
            pac = pyv.split('/')[-1]
            os.environ["PYSPARK_PYTHON"] = "./{0}/bin/python".format(pac)

        self.__sparkcreate(appname, pyv, param=param)

    def __sparkcreate(self, appname=None, pyv=None, **param):
        '''
        @description: create spsrksession
        @param: ex. {spark.executor.memoryOverhead:'4096'}
        @return: spark
        '''
        if not appname:
            appname = 'sparktool_' + \
                datetime.datetime.strftime(datetime.datetime.now(), '%H%M%S')

        conf = SparkConf()
        conf.setAppName(appname)
        for k in param:
            conf.set('"{0}"'.format(str(k)), '"{0}"'.format(str(param[k])))

        if pyv:
            conf.set("spark.sql.execution.arrow.enabled", "true")
            conf.set("spark.yarn.dist.archives", "{0}".format(pyv))

        self.spark = SparkSession.builder.config(conf=conf).getOrCreate()

        print('Create SparkSession: {0}'.format(appname))

    def __sqlparse2sqls(self, sql):
        '''
        @description: parse sql to querys
        '''
        def f(x): return x[:-1] if x[-1] == ';' else x
        for query in sqlparse.split(sql):
            if query.strip() != '':
                yield(f(query))

    def __viewparse2code(self, view):
        '''
        @description: parse view source code
        '''
        hive_sql = 'describe extended {0}'.format(view)
        tt = self.spark.sql(hive_sql).where(
            col('col_name') == "View Text").select('data_type').collect()
        viewcode = sqlparse.format(
            tt[0].data_type, reindent=True, keyword_case='lower', identifier_case='lower', strip_comments=True)
        
        return viewcode

    def __sqlparse2table(self, query):
        '''
        @description: get table name from table
        '''
        plan = self.spark._jsparkSession.sessionState().sqlParser().parsePlan(query)
        plan_string = plan.toString().replace('`.`', '.')
        unr = re.findall(r"UnresolvedRelation `(.*?)`", plan_string)
        cte = re.findall(r"CTE \[(.*?)\]", plan.toString())
        cte = [tt.strip() for tt in cte[0].split(',')] if cte else cte
        schema = set()
        tables = set()
        for table_name in unr:
            if table_name not in cte:
                schema.update([table_name.split('.')[0]])
                tables.update([table_name])

        return schema, tables

    def __sqlparse2views(self, query):
        '''
        @description: parse views in sql
        '''
        def one_round(query):
            schema, tables = self.__sqlparse2table(query)
            temp_sch = schema.difference(self.__customviews['view_sch'])
            if temp_sch:
                self.__customviews['view_sch'].update(temp_sch)
                for sc in temp_sch:
                    temp = self.spark.catalog.listTables(dbName=sc)
                    views = [sc + '.' +
                             x.name for x in temp if x.tableType == 'VIEW']
                    for view in views:
                        self.__customviews['view_tbs'][view] = None

            views_find = []
            for table in tables:
                if table in self.__customviews['view_tbs']:
                    if not self.__customviews['view_tbs'][table]:
                        viewcode = self.__viewparse2code(table)
                        self.__customviews['view_tbs'][table] = viewcode
                    views_find.append(table)

            if views_find:
                for view in views_find:
                    viewcode = '(' + self.__customviews['view_tbs'][view] + ')'
                    query = re.sub(view, viewcode, query, flags=re.I)

            return views_find, query

        views_find = ['start']
        while views_find:
            views_find, query = one_round(query)

        return query

    def batch_kudu2view(self, tables, ifprint=True):
        '''
        @description: kudu table to temporary view
        '''
        if isinstance(tables, str):
            tables = [tables]

        def cov(table, kudu):
            view = table.lower().replace('.', '_')
            self.spark.read \
                      .format('org.apache.kudu.spark.kudu') \
                      .option('kudu.master', self.__kudumaster) \
                      .option('kudu.table', kudu) \
                      .load() \
                      .createOrReplaceTempView(view)

            return view

        tb = pt.PrettyTable()
        tb.field_names = ["Origin Table", "Kudu Table", "If Transform"]

        for table in tables:
            table = table.lower()
            if table not in self.__customkudus:
                hive_cmd = "hive -e \"show tblproperties {0} ('kudu.table_name');\"".format(
                    table)
                cmdreturn = os.popen(hive_cmd).read()
                if cmdreturn and 'kudu.table_name' not in cmdreturn:
                    kudu = cmdreturn.split('\t')[0]
                    view = cov(table, kudu)
                    transform = 'New'
                    self.__customkudus[table] = {
                        'view_name': view,
                        'kuku_name': kudu
                    }
                else:
                    kudu = None
                    view = None
                    transform = 'No'
            else:
                view = self.__customkudus[table]['view_name']
                kudu = self.__customkudus[table]['kuku_name']
                transform = 'Added'

            tb.add_row([table, kudu, transform])

        if ifprint and tables:
            print(tb)

    def batch_excutesql(self, sql, sqlsel=[], ifview=False, ifkudu=True, ifbatchre=False):
        '''
        @description: batch excute
        @return: excutesql
        '''
        querys = sqlparse.format(
            sql, reindent=True, keyword_case='lower', identifier_case='lower', strip_comments=True)
        querys = list(self.__sqlparse2sqls(sql))

        if sqlsel:
            querys = [querys[i] for i in sqlsel]

        if ifview:
            querys = [self.__sqlparse2views(query) for query in querys]

        if ifkudu:
            if not self.__kudumaster:
                raise Exception("Please set kudumaster in %s" % ini)
            tables_set = set()
            for query in querys:
                schema, tables = self.__sqlparse2table(query)
                tables_set.update(tables)

            self.batch_kudu2view(tables_set)

            if self.__customkudus:
                querys_rekudu = []
                for query in querys:
                    for tb in tables_set:
                        tb_lower = tb.lower()
                        if tb_lower in self.__customkudus:
                            query = re.sub(
                                tb, self.__customkudus[tb_lower]['view_name'], query, flags=re.I)
                    querys_rekudu.append(query)

                querys = querys_rekudu

        cnt_query = len(querys)
        i = 0
        temp_batch = []
        for query in querys:
            i += 1
            if i < cnt_query:
                print(
                    '\rExcute Progress: {0}/{1}'.format(i, cnt_query), end='')
            else:
                print('\rExcute Progress: {0}/{1}'.format(i, cnt_query))
            temp = self.spark.sql(query)
            temp_batch.append(temp)

        if ifbatchre:
            return temp_batch if len(temp_batch) > 1 else temp_batch[0]
        else:
            return temp

    def batch_printkudus(self):
        '''
        @description: print tablecovdict
        @return: print
        '''
        tb = pt.PrettyTable()
        tb.field_names = ["Origin Table", "Kudu Table", "Temporary View"]

        if self.__customkudus:
            for table in self.__customkudus:
                kudu = self.__customkudus[table]['kuku_name']
                view = self.__customkudus[table]['view_name']
                transform = 'Added'
                tb.add_row([table, kudu, view])

            print(tb)

        else:
            'There is no transformed table'
