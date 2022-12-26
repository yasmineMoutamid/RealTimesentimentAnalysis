# RealTimesentimentAnalysis

## Objectif :
ce projet est une mise en place d'une architecture Kappa pour le real-time sentiment analysis à l’aide de la source de données Streaming Api twitter en se basant sur les technologies suivantes :
•	Apache Kafka pour la manipulation de flux de données et ingestion.
•	Spark ML pour l’entrainement d’un modèle pour la prédiction en temps réel.
•	Elastic Search pour la recherche et indexation des données rapidement et en temps réel
•	Kibana pour la visualisation en temps réel.
## Architecture Du Projet 
![image](https://user-images.githubusercontent.com/82553919/208305954-621b1fe4-cad0-43e1-8041-c24b094bcbfb.png)


## Installation et mise en place des outils :
-Tout d’abord , on suppose que vous avez déjà installé docker sur votre machine. Si ce n’est pas le cas, vous pouvez télécharger et installer à partir du site officielle : https://docs.docker.com/desktop/install/windows-install/ .

-Ensuite créez un dossier dans votre répertoire(par exemple projet bigdata) et copiez le fichier
« docker-compose.yml » (déjà présent dans le dossier) qui contient l’ensemble des images qui seront crées dans notre projet .Lancez la création et le démarrage des images en tapant la commande : docker-compose -f docker-compose.yml up –d .
 
-On aura comme résultat la création des images docker qu’on peut les vérifier dans la partie images dans Docker Desktop
 
 
-Finalement vériez la liste des conteneurs
 

## Manipulation
### 1.1	Kafka : Ingestion de données

Tout d’abord ,Intallez les packages kafka-python et tweepy que nous aurons besoin

Créez et lancez le producteur Kafka qui permet de se connecter à l'API Twitter et d'obtenir des tweets sur un sujet particulier dans notre cas nous avons choisi python(Code producer.py).
 
### 1.2	Spark ML : Entrainement du modèle

Dans cette partie nous allons entrainer et créer un modele pour la prédiction en temps réel .pour cela nous aurons besoin d’un datset(train..csv que vous le trouvez dans les pièces jointes) comme source de données pour l’entrainement.

Créez un notebook dans lequel on va créer et exécuter notre code et on commence à taper notre code et l’exécuter

 
 


### 1.3	ElasticSearch : recherche et indexation des données
Commencez par tester que elasticsearch fonctionne correctement en accédant via l’url suivant : http://localhost:9200/.

 
Ensuite Revenez au notebook et éxecutez le code de création de nouvelle instance et d’indexation des tweets et leur prédiction.




 
Executez le notebook et vérifier dans l’url : localhost:9200/tweets_mondial (tweets_mondial représente le nom d’index spécifé).

 
### 1.4	Kibana : Visualisation en temps réel
Tout d’abord on va accéder à l’interface de Kibana via l’url suivant : localhost:5601/

Choisissez l’option analytics afin d’explorer et visualiser les données parvenu d’elasticsearch.

Ensuite dans le menu à gauche cliquez sur « Dev Tools »
 
 
Tapez le code suivant afin d’exploiter les données indexés et exécuter pour vérifier le les données s’affiche correctement

A ce niveau on passe au deuxième étape c’est celle de création d’index. Pour se faire cliquez sur
« stack management »puis sur « Index Pattern » dans le menu à gauche comme suit
 
Cliquez sur le bouton en haut « Create indes pattern ».
Choissisez un nom pour votre index pattern.
 
Finalement visualiser notre index pattern.

Maintenant on passe à la création des visualisations dans kibana .Sous la catégorie « analystics » cliquez sur Dashboard.
 
Ensuite cliquez sur le bouton en haut « Create dashboard ».


A ce niveau une interface s’apparait dans lequel nous allons créer nos visualisations
![image](https://user-images.githubusercontent.com/82553919/208305646-bd4a59a7-fa7e-4206-915a-473a9e0338c4.png)

 
Sélectionnez l’index pattern que vous avez créez.

Et commencez à créer vos visualisations en spécifiant à chaque fois le type de visualisation.

Vous pouvez créer un graphe de nombres des predictions negatives et positives
   
![image](https://user-images.githubusercontent.com/82553919/208305523-c376bbbb-51a8-4472-ae44-85a233ee61ae.png)

On peut ainsi créer differentes graphes et visualisations comme suit :

![image](https://user-images.githubusercontent.com/82553919/208305469-968dce93-154e-49a5-87fa-fbf242c18bda.png)

