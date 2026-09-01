
from pyspark.sql import SparkSession

####################################
####### SparkSession.Builder
####################################

def get_spark(appname = 'localspark', engine = 'sail'):

    print()
    print('[get_spark]')
    sparksession_given = SparkSession.getActiveSession()

    if sparksession_given is None:
        print()
        print(f'{"sparksession_given is None:":<22} {sparksession_given is None}')
        print('Spark session does not exist before SparkSession.builder')
        print()
        print('getOrCreate will [Create], not [get] >> ')
        # LP: Create from where: It asks the OS "give me whatever java is on PATH",
        # OR, whatever bin was set to JAVA home.


    else:
        print()
        print("Spark session already provided by environment, validate the carried configs identifies your environment's provision:")
        general_describe_spark(sparksession_given)
        print('getOrCreate will [get], not [Create] >>')

    print()

    if engine == 'sail':
        print()
        global server
        from pysail.spark import SparkConnectServer
        server = SparkConnectServer()
        server.start()
        ip, port = server.listening_address
        sc_address = f'sc://{ip}:{port}'
        spark = SparkSession.builder.appName(appname).remote(f'sc://{ip}:{port}').create()

    elif engine == 'java':
        spark = SparkSession.builder.appName(appname).getOrCreate()

    else:
        raise ValueError(f"engine must be 'sail' or 'java', got {engine!r}")

    # set configs applicable to all enginers
    spark.conf.set('spark.sql.session.timeZone', 'UTC')
    spark.conf.set('spark.sql.ansi.enabled', 'true')

    print()
    print('describe availed spark')
    general_describe_spark(spark)

    print()
    print('get_spark OK')
    print()

    return spark



####################################
####### describe spark
####################################

def general_describe_spark(spark):
    print(f'{"session class":<22} {type(spark).__module__}')
    print(f'{"spark.version":<22} {spark.version}')

    if 'connect' not in type(spark).__module__:
        print(f'{"mode":<22} direct_jvm')
        java = spark.sparkContext._jvm
        java = f"{java.System.getProperty('java.version')} ({java.System.getProperty('java.vendor')})"
        print(f'{"java runtime":<22} {java}')

    if 'connect' in type(spark).__module__:
        print(f'{"mode":<22} spark-connect')
        print(f'{"java runtime is NA":<22} java runtime is unidentifiable in connect mode')
        # SUB: pseudocode in future:
            # engineer the a favourite way to diffrentiate between, pysail, AWS, dataproc and Databricks.
            # for now just use this as i am only on databricks
            # i remember that up till now, i only test on databricks using the if connect in type(sparK?)
        try:
            row = spark.sql('SELECT current_version() AS v').collect()[0][0]
            print('sub: does this work on databricks')
            print('get precise Databricks runtime:')  
            for field in row.asDict():
                print(f'  {field:<18} -> {row[field]}')
        except Exception as e:
            print()
            # print(f'  {"current_version()":<18} -> n/a ({type(e).__name__})')

    print(f'{"conf.getall":<22}')
    for k, v in sorted(spark.conf.getAll.items()):
        v = v if len(v) <= 80 else v[:77] + '...'
        print(f'  {k:<52} {v}')




####################################
####### stop spark
####################################

def stop_spark(spark, verbose=True):
    try:
        spark.stop()
        outcome = 'stopped'
    except Exception as e:
        outcome = f'stop refused ({type(e).__name__})'

    if verbose:
        print(f'[get_spark] {outcome}')

    return outcome




# after a while it seems like, we only really need on spark.conf... reopen this only when i face the two more i need, and if emr and dataproc has any new things
# def sparkconfig_local(spark):
#     spark.conf.set('spark.sql.session.timeZone', 'UTC')  # to strip timestamps of Timezone when mutating
#     spark.conf.set('spark.sql.execution.arrow.pyspark.enabled', 'true') # Accelerates Pandas/PyArrow conversions (eg: write_csv, plotly)
#     spark.sparkContext.setLogLevel('ERROR')

# def sparkconfig_databricks(spark):
#     spark.conf.set('spark.sql.session.timeZone', 'UTC')

# def sparkconfig_sail(spark):
#     spark.conf.set('spark.sql.session.timeZone', 'UTC')  # to strip timestamps of Timezone when mutating

# def sparkconfig_emr():
#     return None
# def sparkconfig_dataproc():
#     return None