# Analyse des ventes avec Apache Spark et Scala

Projet realise a partir de l'enonce « Travaux pratiques — Apache Spark & Scala ». L'application lit le CSV fourni, applique les nettoyages et controles demandes, calcule les indicateurs de vente et exporte un CSV partitionne par annee et mois.

## Contenu du projet

```text
build.sbt
project/build.properties
src/main/scala/fr/master/sparkventes/SalesAnalytics.scala
data/ventes.csv
run.bat
rapport.md
```

## Environnement requis

- JDK 17 ou 21 (21 recommande) ;
- Apache Spark 4.3.0 ;
- sbt 1.13.1 ou une version 1.x compatible.

Le projet fixe Scala 2.13.18 dans `build.sbt`, la version binaire requise par Spark 4. Le compilateur Scala est telecharge par sbt, il n'est pas necessaire de l'installer separement.

Liens officiels : [Apache Spark 4.3.0](https://spark.apache.org/downloads/), [installation de sbt sous Windows](https://www.scala-sbt.org/1.x/docs/Installing-sbt-on-Windows.html), [Scala 2.13.18](https://www.scala-lang.org/download/2.13.18.html).

Verifier les installations dans PowerShell :

```powershell
java -version
sbt --version
spark-submit --version
```

## Compiler et executer sous Windows

Ouvrir un terminal dans le dossier du projet. Le CSV fourni se trouve dans `data/ventes.csv`.

```powershell
.\run.bat
```

Le script compile avec `sbt clean package`, puis lance l'application en local avec Spark. Il accepte aussi les chemins d'entree et de sortie :

```powershell
.\run.bat "C:\Users\USER\Desktop\ventes.csv" "C:\Users\USER\Documents\Codex\sortie_ventes"
```

Les arguments sont :

1. le chemin du CSV source ;
2. le repertoire de sortie.

La commande equivalente sans le script est :

```powershell
sbt clean package
spark-submit --master "local[*]" --class fr.master.sparkventes.SalesAnalytics target\scala-2.13\spark-scala-ventes_2.13-1.0.0.jar data\ventes.csv output
```

## Etapes de traitement

1. Lecture du CSV avec son en-tete ; les valeurs sont lues comme texte pour controler les conversions.
2. Suppression des espaces superflus ; codes produit et moyens de paiement en majuscules ; categories et villes en casse normalisee.
3. Conversion de la date en timestamp, de la quantite en entier et des montants/remises en decimal.
4. Rejet des lignes sans identifiant de transaction/client, date invalide, quantite non positive, prix/cout negatif ou remise hors de `[0, 1]`.
5. Calcul de `gross_amount`, `net_amount_ht`, `total_cost`, `margin` et `margin_rate`.
6. Ajout de `year`, `month`, `day_of_week`, `sale_segment` et `margin_status`.
7. Affichage de totaux et du resume par categorie, puis export en mode overwrite partitionne par `year` et `month`.

`margin_rate` est un pourcentage (par exemple `12.90` signifie 12,90 %). Si le chiffre d'affaires net d'une ligne vaut zero, le taux est laisse vide (`null`) afin d'eviter une division par zero. `day_of_week` suit la convention Spark : 1 = dimanche, 7 = samedi. `margin_status` vaut `NEGATIVE`, `POSITIVE` ou `ZERO`.

## Resultat de l'export

Spark cree un repertoire, pas un fichier CSV unique. Sa structure ressemble a ceci :

```text
output/
  year=2026/
    month=1/
      part-....csv
    month=2/
      part-....csv
  _SUCCESS
```

Chaque `part-*.csv` comporte un en-tete. Les colonnes de partition `year` et `month` apparaissent dans les noms de dossiers ; Spark les reconstitue lors d'une relecture du repertoire.

## Rapport et chiffres de reference

Voir [rapport.md](rapport.md) pour le profil du fichier fourni, les indicateurs calcules et les limites de l'analyse.
