1.打包
windows环境运行：需要包含Hbase 1.1.4的库，以及QuerySrv/lib下的依赖包
Linux环境运行：建议只保留项目编译出来的class，不包含第三方包，放到环境运行的时候把第三方库加在classpath中
<!-- 脚本样例如下：
	CLASSPATH=.:/home/ngqzd/QuerySrv/QuerySrv.jar:/home/hadoop/hbase/conf/hbase-site.xml
	
	jars=`find /home/ngqzd/QuerySrv/lib -name \*.jar`
	for jar in $jars 
	do 
	    CLASSPATH="$CLASSPATH:$jar"
	done
	
	jars=`find /home/hadoop/hbase/lib -name \*.jar`
	for jar in $jars 
	do 
	    CLASSPATH="$CLASSPATH:$jar"
	done
	
	jars=`find /usr/java/latest/jre/lib -name \*.jar`
	for jar in $jars 
	do 
	    CLASSPATH="$CLASSPATH:$jar"
	done
	
	echo $CLASSPATH
	nohup java  -Xmx16g -cp $CLASSPATH com.asiainfo.MainApp /home/ngqzd/QuerySrv/QuerySrv.properties & 
-->

2.配置文件QuerySrv.properties
只需要修改路径、MySql配置(DBPWD经过com.asiainfo.util.PWDHandle加密的)、DEBUG_MODE连Hbase则为false

3.加密类PWDHandle
DBPWD和LOGIN_USERS_PWD的PWD部分均用此类加密
<!-- 运行样例如下：
	java -cp ./QuerySrv.jar com.asiainfo.util.PWDHandle
	---begin---
	输入选项e或者d选择加密或者解密：e
	输入明文：123456
	密文为：JONNAEIDFNHM
	---end---
	---begin---
	输入选项e或者d选择加密或者解密：d
	输入密文：JONNAEIDFNHM
	密文为：123456
	---end---
-->

4.DEBUG_MODE的意义
假如为true，必须指定DEBUG_CDR_FILE并读取文件代替Hbase，且文件编码必须为gbk
假如是清单，文件内容为原始清单
假如是三种账单，支持翻译后账单和原始账务文件格式(即每条账单以START和END作开头和结尾)

