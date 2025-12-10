## Commands
```bash
mvn clean package
spark-submit --class fr.esilv.SparkMain target/Project-1.0-SNAPSHOT.jar daily-integration 2025-01-01 C:/Users/india/Desktop/Cours/SparkBigData/data/dump-2025-01-01.csv
spark-submit --class fr.esilv.SparkMain target/Project-1.0-SNAPSHOT.jar report
spark-submit --class fr.esilv.SparkMain target/Project-1.0-SNAPSHOT.jar recompute-dump 2025-01-02 C:/Users/india/Desktop/Cours/SparkBigData/data/recomposed/2025-01-02/
spark-submit --class fr.esilv.SparkMain target/Project-1.0-SNAPSHOT.jar diff C:/Users/india/Desktop/Cours/SparkBigData/data/recomposed/2025-01-02/ C:/Users/india/Desktop/Cours/SparkBigData/data/recomposed/2025-01-03/

```



## Current todo


## Main Steps

✅ Finaliser pom.xml (Spark + shade plugin).

✅ Créer SparkMain + la structure de packages.

✅ Implémenter DailyIntegrationJob (lecture CSV → diff → bal_diff + bal_latest).

✅ Implémenter ReportJob (aggrégats par département).

✅ Implémenter RecomputeDumpJob (reconstruction à partir de bal_diff).

❌ Implémenter DiffJob (comparaison de deux snapshots parquet).

❌ Écrire les 4 scripts .sh qui appellent spark-submit.

❌ Tester localement avec quelques petits CSV jouets, puis avec le script de test donné.

❌ Supprimer les logs, sauvegarder les outputs, ...