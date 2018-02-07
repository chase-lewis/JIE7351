rmdir /s /q \tmp\kafka-logs
rmdir /s /q \tmp\zookeeper
start cmd /c start-zookeeper.bat
timeout 5
start cmd /c start-kafka.bat
timeout 10
start cmd /c start-schema.bat