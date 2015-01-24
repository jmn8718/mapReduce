rm -r output*
javac  -cp `hadoop classpath` *.java  # compila
jar cvf LogCounter.jar *.class # crea el JAR
hadoop jar LogCounter.jar LogCounter -D excluido=127.0.0.2 input* output
