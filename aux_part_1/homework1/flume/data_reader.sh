cat linux_messages_3000lines.txt | while read line ; do echo "$line" ; sleep 0.2 ; done > output.txt

tail -f /custom_data/output.txt | nc localhost 44444