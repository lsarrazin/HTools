# HBase Tools

## Purpose

## Installation
To be provided

## Repl behavior

### Command line options
```
org.edma.hbasetools.repl.HBaseToolsShell -help
usage: Usage
 -cs,--core-site <arg>    hadoop core-site.xml file path
 -h,--help                Show this help
 -hs,--hbase-site <arg>   hbase-site.xml file path
 -kc,--krb5conf <arg>     krb5.conf file path
 -kt,--keytab <arg>       kerberos principal's keytab to use
 -p,--principal <arg>     kerberos principal to use
 -t,--table-name <arg>    Hbase table name
 -zp,--zk-port <arg>      zookeeper quorum port (default: 2181)
 -zq,--zk-quorum <arg>    zookeeper quorum (ex: host1,host2,host3)
```

### Handling Kerberos
Kerberos is used by default.

## HBaseTools commands

### HBase default instance
When the REPL starts, it creates an HBase wrapper named as hbase using the following code :
```scala
val hbase: org.edma.hbase.HBaseWrapper = HBaseWrapper.create
// Repeat for each command line parameter
hbase.configure(<opt>, <arg>)
```
### HBase connection
To connect to HBase, you must call the connect method, using either :
```scala
// Connecting from kerberos identifiers given through the command line
hbase connect

// Connecting using a different principal and keytab
hbase connect (<keytab>, <principal>)
```
### HBase schema commands
The following commands are defined
```scala
// List namespaces
hbase show databases
hbase show namespaces

// List tables
hbase show tables
```
Alternatively, you can get HBase tables using the list command
```scala
// Get list of available tables
val tables = hbase list
tables.foreach(println)
```

# Readme help -- REMOVE when mastered
Markdown is a lightweight and easy-to-use syntax for styling your writing. It includes conventions for

```markdown
Syntax highlighted code block

# Header 1
## Header 2
### Header 3

- Bulleted
- List

1. Numbered
2. List

**Bold** and _Italic_ and `Code` text

[Link](url) and ![Image](src)
```
For more details see [GitHub Flavored Markdown](https://guides.github.com/features/mastering-markdown/).
