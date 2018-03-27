set base_dir=%~dp0..\..

set CLASSPATH=""
for %%i in (%base_dir%\share\java\ksql*.jar) do (
	set CLASSPATH=%CLASSPATH%;"%%i"
)

java -cp "%base_dir%\share\java\ksql\*" io.confluent.ksql.datagen.DataGen "$@"
