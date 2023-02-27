#! /bin/bash
# Azriel Bachrach and Yehuda Snow

#  JUnit Tests use ports begining with an 8
#  The rest of the demo uses ports begining with a 9

exec &> >(tee output.log) # send output to file

# 1. Build your code using mvn test, thus running your Junit tests
mvn clean test

# run the rest of the script
java -cp target/classes edu.yu.cs.com3800.stage5.demo.Demo5

# If the script hangs, try running it without logs (or set the level in LoggingServer to SEVERE).