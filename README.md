# kafka-spark-mongodb
ensure good connectivity between Apche Kafka and Apache Spark and Redis " consume selected topic data / process the data consume with spark / write the data in MongoDB/ validate test"

#	Application Interaction Kafka/Spark/MongoDB

Cette application a pour but tester les interactions entre Apache kafka et Apache Spark et MongoDB, elle permet :

-	La consommation des données depuis un topic Kafka.
-	Faire des traitements Spark sur ces données.
-	La création d’une base de données. 
-	La création d’une collection dans la base de données créer.
-	Le remplissage de la base de données par les données traitées.
-	La lecture des données écrit.
-	La vérification des données écrit. 
Générer un fichier résultat en format JSON, qui contient les informations du test et son résultat
#	Composants concernés


Composant	   	Version
Spark		2.3.2
Kafka		1.1.1
MongoDB		3.6.11
DC-OS		1.12
Scala		2.11.8
Assembly		0.14.9




# Prérequis 
-	 Avant de lancer l’application vous devez la configurer, cela se fait au niveau du fichier de configuration de l’application, qui est dans le chemin (/src/main/resources/KafkaMongoDB.conf).


# Traitements 
-	Lancer l’application sur le dcos bootstrap avec la commande 
dcos spark --name="spark-2-3" run --submit-args="--conf spark.mesos.containerizer=docker --conf spark.driver.memory=4G --conf spark.cores.max=3 --conf spark.executor.cores=1 --conf spark.executor.memory=4G --conf spark.mesos.executor.docker.forcePullImage=false --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=hdfs:///spark_history  --class kafkaToMongoDB hdfs:///jars/Kafka-Spark-MongoDB-assembly-0.1.jar"
 
# Validation du test 
- Vérifier le statut du test dans le fichier résultat. 
