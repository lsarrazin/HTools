# HBase Tools

# WARNING: Abandonware! Scala REPL has evolved, leading to too much rework on the REPL (and not enough time)

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
When the REPL starts, it creates an HConfiguration and an HShell instance as follows :
```scala
val hconf0 = new org.edma.hbase.HConfiguration
val hsh = org.edma.hbase.HShell.create(hconf0)
```
### HBase connection
To connect to HBase, you must call the connect method, using either :
```scala
// Connecting from kerberos identifiers given through the command line
connect

// Connecting using a different principal and keytab
hsh.connect(<keytab>, <principal>)
```
### HBase schema commands
The following commands are defined
```scala
// List namespaces
hsh show databases
hsh show namespaces

// List tables
hsh show tables
```
Alternatively, you can get HBase tables using the list command
```scala
// Get list of available tables
val tlist: Array[String] = hsh list
tlist.foreach(println)
```

### HBase table commands

```scala
// Working with tables
val tlist = hsh list
hsh.desc tlist(0)
```

### Working with keys

```scala
```

### Working with queries

Queries are created using HQuery class.
A runnable query consists of one or more attribute selection clause (select), one table (from) 

```scala
// Retrieve data from ns:table under row "mykey"
val myQuery = HQuery
  .select "f1:attr1"
  .from "ns:table"
  .where "mykey"
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
