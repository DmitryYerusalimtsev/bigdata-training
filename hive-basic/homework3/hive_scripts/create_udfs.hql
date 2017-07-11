ADD JAR /jars/udfs-1.0-SNAPSHOT.jar;
CREATE FUNCTION user_agent as 'com.dyerus.useragent.UserAgentUDTF';
DESCRIBE FUNCTION user_agent;