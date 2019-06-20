[INFO] Scanning for projects...
[INFO] 
[INFO] ----------------------< com.jaeng.adhesive:core >-----------------------
[INFO] Building core 1.0-SNAPSHOT
[INFO] --------------------------------[ jar ]---------------------------------
[INFO] 
[INFO] --- maven-dependency-plugin:2.8:tree (default-cli) @ core ---
[INFO] com.jaeng.adhesive:core:jar:1.0-SNAPSHOT
[INFO] +- com.jaeng.adhesive:common:jar:1.0-SNAPSHOT:compile
[INFO] |  +- org.apache.httpcomponents:httpclient:jar:4.5:compile
[INFO] |  |  \- org.apache.httpcomponents:httpcore:jar:4.4.1:compile
[INFO] |  \- org.apache.hadoop:hadoop-yarn-server-web-proxy:jar:2.7.2:compile
[INFO] |     +- org.apache.hadoop:hadoop-yarn-server-common:jar:2.7.2:compile
[INFO] |     \- org.apache.hadoop:hadoop-yarn-common:jar:2.7.2:compile
[INFO] |        +- javax.xml.bind:jaxb-api:jar:2.2.2:compile
[INFO] |        |  \- javax.xml.stream:stax-api:jar:1.0-2:compile
[INFO] |        +- com.sun.jersey:jersey-core:jar:1.9:compile
[INFO] |        +- com.sun.jersey:jersey-client:jar:1.9:compile
[INFO] |        +- com.google.inject.extensions:guice-servlet:jar:3.0:compile
[INFO] |        +- com.google.inject:guice:jar:3.0:compile
[INFO] |        |  +- javax.inject:javax.inject:jar:1:compile
[INFO] |        |  \- aopalliance:aopalliance:jar:1.0:compile
[INFO] |        +- com.sun.jersey:jersey-server:jar:1.9:compile
[INFO] |        |  \- asm:asm:jar:3.1:compile
[INFO] |        \- com.sun.jersey.contribs:jersey-guice:jar:1.9:compile
[INFO] +- org.apache.hadoop:hadoop-common:jar:2.7.2:compile
[INFO] |  +- org.apache.hadoop:hadoop-annotations:jar:2.7.2:compile
[INFO] |  |  \- jdk.tools:jdk.tools:jar:1.8:system
[INFO] |  +- commons-cli:commons-cli:jar:1.2:compile
[INFO] |  +- org.apache.commons:commons-math3:jar:3.1.1:compile
[INFO] |  +- xmlenc:xmlenc:jar:0.52:compile
[INFO] |  +- commons-httpclient:commons-httpclient:jar:3.1:compile
[INFO] |  +- commons-codec:commons-codec:jar:1.4:compile
[INFO] |  +- commons-io:commons-io:jar:2.4:compile
[INFO] |  +- commons-net:commons-net:jar:3.1:compile
[INFO] |  +- commons-collections:commons-collections:jar:3.2.2:compile
[INFO] |  +- javax.servlet:servlet-api:jar:2.5:compile
[INFO] |  +- org.mortbay.jetty:jetty:jar:6.1.26:compile
[INFO] |  +- org.mortbay.jetty:jetty-util:jar:6.1.26:compile
[INFO] |  +- javax.servlet.jsp:jsp-api:jar:2.1:runtime
[INFO] |  +- com.sun.jersey:jersey-json:jar:1.9:compile
[INFO] |  |  +- org.codehaus.jettison:jettison:jar:1.1:compile
[INFO] |  |  +- com.sun.xml.bind:jaxb-impl:jar:2.2.3-1:compile
[INFO] |  |  +- org.codehaus.jackson:jackson-jaxrs:jar:1.8.3:compile
[INFO] |  |  \- org.codehaus.jackson:jackson-xc:jar:1.8.3:compile
[INFO] |  +- commons-logging:commons-logging:jar:1.1.3:compile
[INFO] |  +- net.java.dev.jets3t:jets3t:jar:0.9.0:compile
[INFO] |  |  \- com.jamesmurty.utils:java-xmlbuilder:jar:0.4:compile
[INFO] |  +- commons-lang:commons-lang:jar:2.6:compile
[INFO] |  +- commons-configuration:commons-configuration:jar:1.6:compile
[INFO] |  |  +- commons-digester:commons-digester:jar:1.8:compile
[INFO] |  |  |  \- commons-beanutils:commons-beanutils:jar:1.7.0:compile
[INFO] |  |  \- commons-beanutils:commons-beanutils-core:jar:1.8.0:compile
[INFO] |  +- org.slf4j:slf4j-api:jar:1.7.10:compile
[INFO] |  +- org.slf4j:slf4j-log4j12:jar:1.7.10:compile
[INFO] |  +- org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile
[INFO] |  +- org.codehaus.jackson:jackson-mapper-asl:jar:1.9.13:compile
[INFO] |  +- org.apache.avro:avro:jar:1.7.4:compile
[INFO] |  |  \- com.thoughtworks.paranamer:paranamer:jar:2.3:compile
[INFO] |  +- com.google.protobuf:protobuf-java:jar:2.5.0:compile
[INFO] |  +- com.google.code.gson:gson:jar:2.2.4:compile
[INFO] |  +- org.apache.hadoop:hadoop-auth:jar:2.7.2:compile
[INFO] |  |  \- org.apache.directory.server:apacheds-kerberos-codec:jar:2.0.0-M15:compile
[INFO] |  |     +- org.apache.directory.server:apacheds-i18n:jar:2.0.0-M15:compile
[INFO] |  |     +- org.apache.directory.api:api-asn1-api:jar:1.0.0-M20:compile
[INFO] |  |     \- org.apache.directory.api:api-util:jar:1.0.0-M20:compile
[INFO] |  +- com.jcraft:jsch:jar:0.1.42:compile
[INFO] |  +- org.apache.curator:curator-client:jar:2.7.1:compile
[INFO] |  +- org.apache.curator:curator-recipes:jar:2.7.1:compile
[INFO] |  +- com.google.code.findbugs:jsr305:jar:3.0.0:compile
[INFO] |  +- org.apache.htrace:htrace-core:jar:3.1.0-incubating:compile
[INFO] |  +- org.apache.zookeeper:zookeeper:jar:3.4.6:compile
[INFO] |  \- org.apache.commons:commons-compress:jar:1.4.1:compile
[INFO] |     \- org.tukaani:xz:jar:1.0:compile
[INFO] +- org.apache.hadoop:hadoop-hdfs:jar:2.7.2:compile
[INFO] |  +- commons-daemon:commons-daemon:jar:1.0.13:compile
[INFO] |  +- io.netty:netty:jar:3.6.2.Final:compile
[INFO] |  +- io.netty:netty-all:jar:4.0.23.Final:compile
[INFO] |  +- xerces:xercesImpl:jar:2.9.1:compile
[INFO] |  |  \- xml-apis:xml-apis:jar:1.3.04:compile
[INFO] |  \- org.fusesource.leveldbjni:leveldbjni-all:jar:1.8:compile
[INFO] +- org.apache.hadoop:hadoop-client:jar:2.7.2:compile
[INFO] |  +- org.apache.hadoop:hadoop-mapreduce-client-app:jar:2.7.2:compile
[INFO] |  |  +- org.apache.hadoop:hadoop-mapreduce-client-common:jar:2.7.2:compile
[INFO] |  |  |  \- org.apache.hadoop:hadoop-yarn-client:jar:2.7.2:compile
[INFO] |  |  \- org.apache.hadoop:hadoop-mapreduce-client-shuffle:jar:2.7.2:compile
[INFO] |  +- org.apache.hadoop:hadoop-yarn-api:jar:2.7.2:compile
[INFO] |  +- org.apache.hadoop:hadoop-mapreduce-client-core:jar:2.7.2:compile
[INFO] |  \- org.apache.hadoop:hadoop-mapreduce-client-jobclient:jar:2.7.2:compile
[INFO] +- org.apache.spark:spark-core_2.11:jar:2.1.1:compile
[INFO] |  +- org.apache.avro:avro-mapred:jar:hadoop2:1.7.7:compile
[INFO] |  |  +- org.apache.avro:avro-ipc:jar:1.7.7:compile
[INFO] |  |  \- org.apache.avro:avro-ipc:jar:tests:1.7.7:compile
[INFO] |  +- com.twitter:chill_2.11:jar:0.8.0:compile
[INFO] |  |  \- com.esotericsoftware:kryo-shaded:jar:3.0.3:compile
[INFO] |  |     +- com.esotericsoftware:minlog:jar:1.3.0:compile
[INFO] |  |     \- org.objenesis:objenesis:jar:2.1:compile
[INFO] |  +- com.twitter:chill-java:jar:0.8.0:compile
[INFO] |  +- org.apache.xbean:xbean-asm5-shaded:jar:4.4:compile
[INFO] |  +- org.apache.spark:spark-launcher_2.11:jar:2.1.1:compile
[INFO] |  +- org.apache.spark:spark-network-common_2.11:jar:2.1.1:compile
[INFO] |  |  \- com.fasterxml.jackson.core:jackson-annotations:jar:2.6.5:compile
[INFO] |  +- org.apache.spark:spark-network-shuffle_2.11:jar:2.1.1:compile
[INFO] |  +- org.apache.spark:spark-unsafe_2.11:jar:2.1.1:compile
[INFO] |  +- javax.servlet:javax.servlet-api:jar:3.1.0:compile
[INFO] |  +- org.apache.commons:commons-lang3:jar:3.5:compile
[INFO] |  +- org.slf4j:jul-to-slf4j:jar:1.7.16:compile
[INFO] |  +- org.slf4j:jcl-over-slf4j:jar:1.7.16:compile
[INFO] |  +- com.ning:compress-lzf:jar:1.0.3:compile
[INFO] |  +- org.xerial.snappy:snappy-java:jar:1.1.2.6:compile
[INFO] |  +- net.jpountz.lz4:lz4:jar:1.3.0:compile
[INFO] |  +- org.roaringbitmap:RoaringBitmap:jar:0.5.11:compile
[INFO] |  +- org.scala-lang:scala-library:jar:2.11.8:compile
[INFO] |  +- org.json4s:json4s-jackson_2.11:jar:3.2.11:compile
[INFO] |  |  \- org.json4s:json4s-core_2.11:jar:3.2.11:compile
[INFO] |  |     +- org.json4s:json4s-ast_2.11:jar:3.2.11:compile
[INFO] |  |     \- org.scala-lang:scalap:jar:2.11.0:compile
[INFO] |  |        \- org.scala-lang:scala-compiler:jar:2.11.0:compile
[INFO] |  |           +- org.scala-lang.modules:scala-xml_2.11:jar:1.0.1:compile
[INFO] |  |           \- org.scala-lang.modules:scala-parser-combinators_2.11:jar:1.0.1:compile
[INFO] |  +- org.glassfish.jersey.core:jersey-client:jar:2.22.2:compile
[INFO] |  |  +- javax.ws.rs:javax.ws.rs-api:jar:2.0.1:compile
[INFO] |  |  +- org.glassfish.hk2:hk2-api:jar:2.4.0-b34:compile
[INFO] |  |  |  +- org.glassfish.hk2:hk2-utils:jar:2.4.0-b34:compile
[INFO] |  |  |  \- org.glassfish.hk2.external:aopalliance-repackaged:jar:2.4.0-b34:compile
[INFO] |  |  +- org.glassfish.hk2.external:javax.inject:jar:2.4.0-b34:compile
[INFO] |  |  \- org.glassfish.hk2:hk2-locator:jar:2.4.0-b34:compile
[INFO] |  |     \- org.javassist:javassist:jar:3.18.1-GA:compile
[INFO] |  +- org.glassfish.jersey.core:jersey-common:jar:2.22.2:compile
[INFO] |  |  +- javax.annotation:javax.annotation-api:jar:1.2:compile
[INFO] |  |  +- org.glassfish.jersey.bundles.repackaged:jersey-guava:jar:2.22.2:compile
[INFO] |  |  \- org.glassfish.hk2:osgi-resource-locator:jar:1.0.1:compile
[INFO] |  +- org.glassfish.jersey.core:jersey-server:jar:2.22.2:compile
[INFO] |  |  +- org.glassfish.jersey.media:jersey-media-jaxb:jar:2.22.2:compile
[INFO] |  |  \- javax.validation:validation-api:jar:1.1.0.Final:compile
[INFO] |  +- org.glassfish.jersey.containers:jersey-container-servlet:jar:2.22.2:compile
[INFO] |  +- org.glassfish.jersey.containers:jersey-container-servlet-core:jar:2.22.2:compile
[INFO] |  +- com.clearspring.analytics:stream:jar:2.7.0:compile
[INFO] |  +- io.dropwizard.metrics:metrics-core:jar:3.1.2:compile
[INFO] |  +- io.dropwizard.metrics:metrics-jvm:jar:3.1.2:compile
[INFO] |  +- io.dropwizard.metrics:metrics-json:jar:3.1.2:compile
[INFO] |  +- io.dropwizard.metrics:metrics-graphite:jar:3.1.2:compile
[INFO] |  +- com.fasterxml.jackson.core:jackson-databind:jar:2.6.5:compile
[INFO] |  |  \- com.fasterxml.jackson.core:jackson-core:jar:2.6.5:compile
[INFO] |  +- com.fasterxml.jackson.module:jackson-module-scala_2.11:jar:2.6.5:compile
[INFO] |  |  +- org.scala-lang:scala-reflect:jar:2.11.7:compile
[INFO] |  |  \- com.fasterxml.jackson.module:jackson-module-paranamer:jar:2.6.5:compile
[INFO] |  +- org.apache.ivy:ivy:jar:2.4.0:compile
[INFO] |  +- oro:oro:jar:2.0.8:compile
[INFO] |  +- net.razorvine:pyrolite:jar:4.13:compile
[INFO] |  +- net.sf.py4j:py4j:jar:0.10.4:compile
[INFO] |  +- org.apache.spark:spark-tags_2.11:jar:2.1.1:compile
[INFO] |  +- org.apache.commons:commons-crypto:jar:1.0.0:compile
[INFO] |  \- org.spark-project.spark:unused:jar:1.0.0:compile
[INFO] +- org.apache.spark:spark-sql_2.11:jar:2.1.1:compile
[INFO] |  +- com.univocity:univocity-parsers:jar:2.2.1:compile
[INFO] |  +- org.apache.spark:spark-sketch_2.11:jar:2.1.1:compile
[INFO] |  +- org.apache.spark:spark-catalyst_2.11:jar:2.1.1:compile
[INFO] |  |  +- org.codehaus.janino:janino:jar:3.0.0:compile
[INFO] |  |  +- org.codehaus.janino:commons-compiler:jar:3.0.0:compile
[INFO] |  |  \- org.antlr:antlr4-runtime:jar:4.5.3:compile
[INFO] |  +- org.apache.parquet:parquet-column:jar:1.8.1:compile
[INFO] |  |  +- org.apache.parquet:parquet-common:jar:1.8.1:compile
[INFO] |  |  \- org.apache.parquet:parquet-encoding:jar:1.8.1:compile
[INFO] |  \- org.apache.parquet:parquet-hadoop:jar:1.8.1:compile
[INFO] |     +- org.apache.parquet:parquet-format:jar:2.3.0-incubating:compile
[INFO] |     \- org.apache.parquet:parquet-jackson:jar:1.8.1:compile
[INFO] +- org.mongodb.spark:mongo-spark-connector_2.11:jar:2.1.1:compile
[INFO] |  \- org.mongodb:mongo-java-driver:jar:3.4.2:compile
[INFO] +- com.ibeetl:beetl:jar:2.9.8:compile
[INFO] +- org.apache.kudu:kudu-spark2_2.11:jar:1.6.0:compile
[INFO] |  \- org.apache.yetus:audience-annotations:jar:0.4.0:compile
[INFO] +- com.google.guava:guava:jar:23.0:compile
[INFO] |  +- com.google.errorprone:error_prone_annotations:jar:2.0.18:compile
[INFO] |  +- com.google.j2objc:j2objc-annotations:jar:1.1:compile
[INFO] |  \- org.codehaus.mojo:animal-sniffer-annotations:jar:1.14:compile
[INFO] +- com.alibaba:fastjson:jar:1.2.6:compile
[INFO] +- org.postgresql:postgresql:jar:42.2.5:compile
[INFO] +- mysql:mysql-connector-java:jar:5.1.6:compile
[INFO] +- org.apache.hive:hive-jdbc:jar:1.1.0:compile
[INFO] |  +- org.apache.hive:hive-common:jar:1.1.0:compile
[INFO] |  |  +- log4j:apache-log4j-extras:jar:1.2.17:compile
[INFO] |  |  \- org.apache.ant:ant:jar:1.9.1:compile
[INFO] |  |     \- org.apache.ant:ant-launcher:jar:1.9.1:compile
[INFO] |  +- org.apache.hive:hive-service:jar:1.1.0:compile
[INFO] |  |  +- net.sf.jpam:jpam:jar:1.1:compile
[INFO] |  |  +- org.eclipse.jetty.aggregate:jetty-all:jar:7.6.0.v20120127:compile
[INFO] |  |  |  +- org.apache.geronimo.specs:geronimo-jta_1.1_spec:jar:1.1.1:compile
[INFO] |  |  |  +- javax.mail:mail:jar:1.4.1:compile
[INFO] |  |  |  +- javax.activation:activation:jar:1.1:compile
[INFO] |  |  |  +- org.apache.geronimo.specs:geronimo-jaspic_1.0_spec:jar:1.0:compile
[INFO] |  |  |  +- org.apache.geronimo.specs:geronimo-annotation_1.0_spec:jar:1.1.1:compile
[INFO] |  |  |  \- asm:asm-commons:jar:3.1:compile
[INFO] |  |  |     \- asm:asm-tree:jar:3.1:compile
[INFO] |  |  \- org.apache.thrift:libfb303:jar:0.9.2:compile
[INFO] |  +- org.apache.hive:hive-serde:jar:1.1.0:compile
[INFO] |  |  +- net.sf.opencsv:opencsv:jar:2.3:compile
[INFO] |  |  \- com.twitter:parquet-hadoop-bundle:jar:1.6.0rc3:compile
[INFO] |  +- org.apache.hive:hive-metastore:jar:1.1.0:compile
[INFO] |  |  +- com.jolbox:bonecp:jar:0.8.0.RELEASE:compile
[INFO] |  |  +- org.apache.derby:derby:jar:10.11.1.1:compile
[INFO] |  |  +- org.datanucleus:datanucleus-api-jdo:jar:3.2.6:compile
[INFO] |  |  +- org.datanucleus:datanucleus-core:jar:3.2.10:compile
[INFO] |  |  +- org.datanucleus:datanucleus-rdbms:jar:3.2.9:compile
[INFO] |  |  +- commons-pool:commons-pool:jar:1.5.4:compile
[INFO] |  |  +- commons-dbcp:commons-dbcp:jar:1.4:compile
[INFO] |  |  +- javax.jdo:jdo-api:jar:3.0.1:compile
[INFO] |  |  |  \- javax.transaction:jta:jar:1.1:compile
[INFO] |  |  \- org.antlr:antlr-runtime:jar:3.4:compile
[INFO] |  |     +- org.antlr:stringtemplate:jar:3.2.1:compile
[INFO] |  |     \- antlr:antlr:jar:2.7.7:compile
[INFO] |  +- org.apache.thrift:libthrift:jar:0.9.2:compile
[INFO] |  \- org.apache.curator:curator-framework:jar:2.6.0:compile
[INFO] +- org.apache.kafka:kafka-clients:jar:1.0.0:compile
[INFO] |  \- org.lz4:lz4-java:jar:1.4:compile
[INFO] +- log4j:log4j:jar:1.2.16:compile
[INFO] \- com.sun.xml.fastinfoset:FastInfoset:jar:1.2.2:compile
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 2.236 s
[INFO] Finished at: 2019-06-18T20:18:54+08:00
[INFO] ------------------------------------------------------------------------
