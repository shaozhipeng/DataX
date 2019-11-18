#!/usr/bin/env python
# -*- coding:utf-8 -*-

import codecs
import json
import os
import platform
import re
import signal
import socket
import subprocess
import sys
import time
from optparse import OptionGroup
from optparse import OptionParser
from string import Template


# 判断当前执行datax.py所在的操作系统
def isWindows():
    return platform.system() == 'Windows'


# datax的安装目录
DATAX_HOME = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# datax的版本，开源3.0
DATAX_VERSION = 'DATAX-OPENSOURCE-3.0'

if isWindows():
    # 注册一个codec的搜索函数。
    # 搜索函数要求能输入一个参数，编码器的名称以小写字母命名，如果成功找到则返回CodecInfo对象，否则返回None。
    codecs.register(lambda name: name == 'cp65001' and codecs.lookup('utf-8') or None)
    # datax依赖的jar包放到环境变量里
    CLASS_PATH = ("%s/lib/*") % (DATAX_HOME)
else:
    # :. 包括当前路径
    CLASS_PATH = ("%s/lib/*:.") % (DATAX_HOME)

# 日志配置文件
LOGBACK_FILE = ("%s/conf/logback.xml") % (DATAX_HOME)

# 默认的JVM参数：JVM启动时分配的内存和JVM运行过程中能够使用的最大内存均为1G，并指定dump文件存储路径
DEFAULT_JVM = "-Xms1g -Xmx1g -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=%s/log" % (DATAX_HOME)

# 默认启动参数：文件编码
DEFAULT_PROPERTY_CONF = "-Dfile.encoding=UTF-8 -Dlogback.statusListenerClass=ch.qos.logback.core.status.NopStatusListener -Djava.security.egd=file:///dev/urandom -Ddatax.home=%s -Dlogback.configurationFile=%s" % (
    DATAX_HOME, LOGBACK_FILE)

# 启动datax引擎命令
# 选择"server"VM，默认VM是server，因为是在服务器类计算机上运行
# zip/jar
# com.alibaba.datax.core.Engine为Main方法入口的主类
ENGINE_COMMAND = "java -server ${jvm} %s -classpath %s  ${params} com.alibaba.datax.core.Engine -mode ${mode} -jobid ${jobid} -job ${job}" % (
    DEFAULT_PROPERTY_CONF, CLASS_PATH)

# 远程DEBUG配置
REMOTE_DEBUG_CONFIG = "-Xdebug -Xrunjdwp:transport=dt_socket,server=y,address=9999"

# 运行结果状态码
RET_STATE = {
    "KILL": 143,
    "FAIL": -1,
    "OK": 0,
    "RUN": 1,
    "RETRY": 2
}


# 获取运行datax.py所在机器的IP地址
def getLocalIp():
    try:
        return socket.gethostbyname(socket.getfqdn(socket.gethostname()))
    except:
        return "Unknown"


# kill self
def suicide(signum, e):
    global child_process
    print >> sys.stderr, "[Error] DataX receive unexpected signal %d, starts to suicide." % (signum)

    if child_process:
        child_process.send_signal(signal.SIGQUIT)
        time.sleep(1)
        child_process.kill()
    print >> sys.stderr, "DataX Process was killed ! you did ?"
    sys.exit(RET_STATE["KILL"])


def register_signal():
    if not isWindows():
        global child_process
        # signal程序内部的信号处理
        # SIGINT信号编号为2 Ctrl+C=>终止进程信号
        signal.signal(2, suicide)
        # nohup 守护进程发出的关闭信号
        signal.signal(3, suicide)
        # 命令行 kill pid 时的信号
        signal.signal(15, suicide)


# 解析命令行参数
def getOptionParser():
    # %prog == datax.py
    usage = "usage: %prog [options] job-url-or-path"
    parser = OptionParser(usage=usage)

    # 生产环境参数
    prodEnvOptionGroup = OptionGroup(parser, "Product Env Options",
                                     "Normal user use these options to set jvm parameters, job runtime mode etc. "
                                     "Make sure these options can be used in Product Env.")

    # jvm参数
    prodEnvOptionGroup.add_option("-j", "--jvm", metavar="<jvm parameters>", dest="jvmParameters", action="store",
                                  default=DEFAULT_JVM, help="Set jvm parameters if necessary.")

    # jobid，Long型；standalone模式下jobid可以为默认值-1；否则不能为-1
    prodEnvOptionGroup.add_option("--jobid", metavar="<job unique id>", dest="jobid", action="store", default="-1",
                                  help="Set job unique id when running by Distribute/Local Mode.")

    # job运行模式
    prodEnvOptionGroup.add_option("-m", "--mode", metavar="<job runtime mode>",
                                  action="store", default="standalone",
                                  help="Set job runtime mode such as: standalone, local, distribute. "
                                       "Default mode is standalone.")

    # json插件文件中的参数
    prodEnvOptionGroup.add_option("-p", "--params", metavar="<parameter used in job config>",
                                  action="store", dest="params",
                                  help='Set job parameter, eg: the source tableName you want to set it by command, '
                                       'then you can use like this: -p"-DtableName=your-table-name", '
                                       'if you have mutiple parameters: -p"-DtableName=your-table-name -DcolumnName=your-column-name".'
                                       'Note: you should config in you job tableName with ${tableName}.')

    # reader插件名
    prodEnvOptionGroup.add_option("-r", "--reader", metavar="<parameter used in view job config[reader] template>",
                                  action="store", dest="reader", type="string",
                                  help='View job config[reader] template, eg: mysqlreader,streamreader')

    # writer插件名
    prodEnvOptionGroup.add_option("-w", "--writer", metavar="<parameter used in view job config[writer] template>",
                                  action="store", dest="writer", type="string",
                                  help='View job config[writer] template, eg: mysqlwriter,streamwriter')

    parser.add_option_group(prodEnvOptionGroup)

    # 开发/调试模式参数
    devEnvOptionGroup = OptionGroup(parser, "Develop/Debug Options",
                                    "Developer use these options to trace more details of DataX.")

    # 远程调试模式
    devEnvOptionGroup.add_option("-d", "--debug", dest="remoteDebug", action="store_true",
                                 help="Set to remote debug mode.")

    # 日志级别
    devEnvOptionGroup.add_option("--loglevel", metavar="<log level>", dest="loglevel", action="store",
                                 default="info", help="Set log level such as: debug, info, all etc.")
    parser.add_option_group(devEnvOptionGroup)
    return parser


# 生成json模板
def generateJobConfigTemplate(reader, writer):
    # 提示参考文档
    readerRef = "Please refer to the %s document:\n     https://github.com/alibaba/DataX/blob/master/%s/doc/%s.md \n" % (
        reader, reader, reader)
    writerRef = "Please refer to the %s document:\n     https://github.com/alibaba/DataX/blob/master/%s/doc/%s.md \n " % (
        writer, writer, writer)
    print readerRef
    print writerRef

    # 提示执行datax任务的执行命令
    jobGuid = 'Please save the following configuration as a json file and  use\n     python {DATAX_HOME}/bin/datax.py {JSON_FILE_NAME}.json \nto run the job.\n'
    print jobGuid

    # json模板
    jobTemplate = {
        "job": {
            "setting": {
                "speed": {
                    "channel": ""
                }
            },
            "content": [
                {
                    "reader": {},
                    "writer": {}
                }
            ]
        }
    }

    # 找到插件所在路径下的模板文件
    readerTemplatePath = "%s/plugin/reader/%s/plugin_job_template.json" % (DATAX_HOME, reader)
    writerTemplatePath = "%s/plugin/writer/%s/plugin_job_template.json" % (DATAX_HOME, writer)
    try:
        # 加载为json对象
        readerPar = readPluginTemplate(readerTemplatePath);
    except Exception, e:
        print "Read reader[%s] template error: can\'t find file %s" % (reader, readerTemplatePath)
    try:
        # 加载为json对象
        writerPar = readPluginTemplate(writerTemplatePath);
    except Exception, e:
        print "Read writer[%s] template error: : can\'t find file %s" % (writer, writerTemplatePath)

    # 替换为插件提供的模板
    jobTemplate['job']['content'][0]['reader'] = readerPar;
    jobTemplate['job']['content'][0]['writer'] = writerPar;
    print json.dumps(jobTemplate, indent=4, sort_keys=True)


def readPluginTemplate(plugin):
    with open(plugin, 'r') as f:
        return json.load(f)


def isUrl(path):
    if not path:
        return False

    assert (isinstance(path, str))
    # 正则表达式匹配json路径是否是url
    m = re.match(r"^http[s]?://\S+\w*", path.lower())
    if m:
        return True
    else:
        return False


# 构建datax的启动命令
def buildStartCommand(options, args):
    commandMap = {}
    # 默认的JVM
    tempJVMCommand = DEFAULT_JVM

    # 命令行参数的JVM
    if options.jvmParameters:
        # 这里是拼接起来
        # -Xms1g -Xmx1g -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/Users/shaozhipeng/DataX/core/src/main/log -Xms2g -Xmx2g -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=./log
        tempJVMCommand = tempJVMCommand + " " + options.jvmParameters

    # 远程DEBUG参数
    if options.remoteDebug:
        tempJVMCommand = tempJVMCommand + " " + REMOTE_DEBUG_CONFIG
        print 'local ip: ', getLocalIp()

    # 日志级别
    if options.loglevel:
        tempJVMCommand = tempJVMCommand + " " + ("-Dloglevel=%s" % (options.loglevel))

    # 运行模式
    if options.mode:
        commandMap["mode"] = options.mode

    # jobResource 可能是 URL，也可能是本地文件路径（相对,绝对）
    jobResource = args[0]
    if not isUrl(jobResource):
        jobResource = os.path.abspath(jobResource)
        if jobResource.lower().startswith("file://"):
            jobResource = jobResource[len("file://"):]

    jobParams = ("-Dlog.file.name=%s") % (jobResource[-20:].replace('/', '_').replace('.', '_'))
    if options.params:
        jobParams = jobParams + " " + options.params

    if options.jobid:
        commandMap["jobid"] = options.jobid

    commandMap["jvm"] = tempJVMCommand
    commandMap["params"] = jobParams
    commandMap["job"] = jobResource

    # 将字符串的格式固定下来，重复利用，给字符串里的${}赋值
    return Template(ENGINE_COMMAND).substitute(**commandMap)


# python datax.py运行时，控制台输出的版权信息
def printCopyright():
    print '''
DataX (%s), From Alibaba !
Copyright (C) 2010-2017, Alibaba Group. All Rights Reserved.

''' % DATAX_VERSION
    sys.stdout.flush()


if __name__ == "__main__":
    # 版权信息
    printCopyright()
    # 解析命令行参数
    parser = getOptionParser()

    # {'jvmParameters': '', 'loglevel': 'info', 'writer': None, 'jobid': '-1', 'remoteDebug': None, 'params': None, 'mode': 'standalone', 'reader': None}
    # []
    options, args = parser.parse_args(sys.argv[1:])
    print options
    print args

    # 如果reader和writer均不为空，生成对应的json模板
    if options.reader is not None and options.writer is not None:
        generateJobConfigTemplate(options.reader, options.writer)
        sys.exit(RET_STATE['OK'])
    # 后面只跟一个json文件路径，所以执行同步任务时，len(args) == 1
    if len(args) != 1:
        # 打印帮助信息
        parser.print_help()
        sys.exit(RET_STATE['FAIL'])

    # 前面两个if...=>执行同步任务时，命令行中不要同时有reader和writer；带上一个原始args-json文件路径
    # buildStartCommand之后是默认参数和命令行获取到的参数拼接在一块的
    startCommand = buildStartCommand(options, args)
    # print startCommand

    # 使用subprocess打开一个新的进程，可以理解为执行shell脚本（shell=True）
    child_process = subprocess.Popen(startCommand, shell=True)
    register_signal()
    # 和子进程交互：发送数据到stdin，并从stdout和stderr读数据，直到收到EOF。等待子进程结束。
    (stdout, stderr) = child_process.communicate()

    sys.exit(child_process.returncode)
